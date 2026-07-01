defmodule Instashard.Proxy.ClientSession do
  @moduledoc """
  One GenServer per client connection.

  Extended protocol pipeline model:
    P/B/D/E/C/H → send to backend immediately, NO recv
    S (Sync)     → send to backend, recv all responses until ReadyForQuery, forward to client

  Prepared statement rewrite:
    Parse: hash SQL → internal_name (StmtCache), rewrite stmt name, update stmt_map
    Bind:  rewrite stmt name field using stmt_map
    stmt_set per socket tracks which internal names are already prepared
    If already in stmt_set, skip sending Parse (backend already has it)

  Transaction lazy checkout:
    BEGIN → fake response, buffer packet
    First shard-bearing stmt → checkout, flush buffer (drain responses), lock socket
    COMMIT/ROLLBACK → forward on tx_socket, checkin on ReadyForQuery('I')
  """

  use GenServer
  require Logger

  alias Instashard.Backend.Pool
  alias Instashard.Backend.StmtCache
  alias Instashard.Proxy.PgProtocol
  alias Instashard.Proxy.ShardRouter

  defstruct [
    client_socket: nil,
    tx_socket: nil,
    tx_shard: nil,
    tx_buffer: [],
    tx_entry: nil,
    ext_socket: nil,
    ext_shard: nil,
    ext_entry: nil,
    # client_stmt_name → {internal_name, shard}
    # shard is needed when Bind arrives without a preceding Parse in this pipeline
    stmt_map: %{},
    # How many Parse messages were skipped (stmt already on socket).
    # We inject this many fake ParseComplete packets before forwarding Sync response.
    pending_fake_parses: 0,
    # How many extra ParseComplete responses to drain (not forward) at Sync.
    # Happens when handle_bind re-parses on a new socket — backend will send ParseComplete
    # but the client never sent a Parse so it doesn't expect one.
    pending_drain_parses: 0
  ]

  def start_link(client_socket) do
    GenServer.start_link(__MODULE__, client_socket)
  end

  @impl true
  def init(client_socket) do
    :inet.setopts(client_socket, active: true)
    {:ok, %__MODULE__{client_socket: client_socket}}
  end

  # ── TCP events ────────────────────────────────────────────────────────

  @impl true
  def handle_info({:tcp, _sock, data}, state) do
    {:noreply, dispatch_all(data, state)}
  end

  def handle_info({:tcp_closed, _}, state) do
    Logger.info("[Session] Client disconnected")
    cleanup(state)
    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, _, reason}, state) do
    Logger.error("[Session] TCP error: #{inspect(reason)}")
    cleanup(state)
    {:stop, :normal, state}
  end

  # ── Message splitter ──────────────────────────────────────────────────
  # One TCP segment can carry multiple PG messages. Process each in order.

  # SSL request (no type byte, fixed 8 bytes)
  defp dispatch_all(<<0, 0, 0, 8, 4, 210, 22, 47, rest::binary>>, state) do
    state = handle_packet(<<0, 0, 0, 8, 4, 210, 22, 47>>, state)
    dispatch_all(rest, state)
  end

  # Startup message (no type byte, length-prefixed)
  defp dispatch_all(<<len::32, 0, 3, 0, 0, _::binary>> = data, state) do
    msg_len = len  # len includes the 4-byte length field
    <<msg::binary-size(msg_len), rest::binary>> = data
    state = handle_packet(msg, state)
    dispatch_all(rest, state)
  end

  # Normal message: type(1) + len(4, includes itself) + body(len-4)
  defp dispatch_all(<<type, len::32, rest::binary>>, state) when len >= 4 do
    body_len = len - 4
    case rest do
      <<body::binary-size(body_len), remaining::binary>> ->
        msg = <<type, len::32, body::binary>>
        state = handle_packet(msg, state)
        dispatch_all(remaining, state)
      _ ->
        handle_packet(<<type, len::32, rest::binary>>, state)
    end
  end

  defp dispatch_all(<<>>, state), do: state
  defp dispatch_all(data, state), do: handle_packet(data, state)

  # ── Packet dispatch ───────────────────────────────────────────────────

  defp handle_packet(<<0, 0, 0, 8, 4, 210, 22, 47>>, state) do
    :gen_tcp.send(state.client_socket, "N")
    state
  end

  defp handle_packet(<<_len::32, 0, 3, 0, 0, payload::binary>>, state) do
    params = PgProtocol.parse_startup_params(payload)
    Logger.info("[Session] Startup user=#{params["user"]} db=#{params["database"]}")
    send_auth_ok(state.client_socket)
    state
  end

  defp handle_packet(<<?Q, size::32, rest::binary>> = raw, state) do
    sql_len = size - 4 - 1
    <<sql::binary-size(sql_len), _::8>> = rest
    Logger.info("[Session] SimpleQuery: #{sql}")
    handle_simple_query(sql, raw, state)
  end

  defp handle_packet(<<?P, _::32, _::binary>> = raw, state) do
    sql = PgProtocol.parse_sql_from_parse(raw)
    Logger.debug("[Session] Parse sql=#{sql}")
    handle_parse(sql, raw, state)
  end

  defp handle_packet(<<?B, _::32, _::binary>> = raw, state), do: handle_bind(raw, state)
  defp handle_packet(<<?D, _::32, _::binary>> = raw, state), do: handle_describe(raw, state)
  defp handle_packet(<<?E, _::32, _::binary>> = raw, state), do: handle_forward_ext(raw, state)
  defp handle_packet(<<?C, _::32, _::binary>> = raw, state), do: handle_close(raw, state)
  defp handle_packet(<<?H, 4::32>> = raw, state),            do: handle_flush(raw, state)
  defp handle_packet(<<?S, 4::32>> = raw, state),            do: handle_sync(raw, state)

  defp handle_packet(<<?X, _::32>>, state) do
    cleanup(state)
    state
  end

  defp handle_packet(packet, state) do
    Logger.debug("[Session] Unhandled packet type=0x#{Integer.to_string(:binary.first(packet), 16)}")
    state
  end

  # ── Simple Query ──────────────────────────────────────────────────────

  defp handle_simple_query(sql, raw, state) do
    cond do
      state.tx_socket != nil ->
        forward_on_tx_socket(sql, raw, state)

      begin?(sql) ->
        send_command_complete(state.client_socket, "BEGIN")
        send_ready_for_query(state.client_socket, :in_transaction)
        %{state | tx_buffer: [raw]}

      state.tx_buffer != [] ->
        case ShardRouter.extract_shard(sql) do
          {:ok, shard} ->
            new_state = flush_tx_buffer(shard, state)
            if new_state.tx_socket != nil do
              :ok = :gen_tcp.send(new_state.tx_socket, raw)
              {:ok, _} = forward_response(new_state.tx_socket, state.client_socket)
              new_state
            else
              new_state
            end

          :no_shard ->
            send_ready_for_query(state.client_socket, :in_transaction)
            %{state | tx_buffer: state.tx_buffer ++ [raw]}
        end

      true ->
        route_simple_query(sql, raw, state)
        state
    end
  end

  defp route_simple_query(sql, raw, state) do
    case ShardRouter.extract_shard(sql) do
      {:ok, shard} ->
        case Pool.checkout(shard) do
          {:ok, entry} ->
            {socket, _pc, _ss} = entry
            :ok = :gen_tcp.send(socket, raw)
            forward_response(socket, state.client_socket)
            Pool.checkin(shard, entry)
          {:error, :empty} ->
            Logger.error("[Session] Pool empty for #{shard}")
            send_error(state.client_socket, "pool exhausted for shard #{shard}")
        end
      :no_shard ->
        send_error(state.client_socket, "cannot route: no shard reference in query")
    end
  end

  defp forward_on_tx_socket(sql, raw, state) do
    :ok = :gen_tcp.send(state.tx_socket, raw)
    {:ok, status} = forward_response(state.tx_socket, state.client_socket)
    if status == :idle do
      Logger.info("[Session] TX ended (#{sql}), checking in socket")
      Pool.checkin(state.tx_shard, state.tx_entry)
      %{state | tx_socket: nil, tx_shard: nil, tx_buffer: [], tx_entry: nil}
    else
      state
    end
  end

  # ── Extended: Parse ───────────────────────────────────────────────────
  # Only sends to backend — NO recv. Responses are read at Sync.

  defp handle_parse(sql, raw, state) do
    cond do
      state.tx_socket != nil ->
        {new_stmt_map, new_entry, action} = send_parse(raw, sql, state.tx_shard, state.tx_socket, state.tx_entry, state.stmt_map)
        fakes = state.pending_fake_parses + if(action == :skipped, do: 1, else: 0)
        %{state | stmt_map: new_stmt_map, tx_entry: new_entry, pending_fake_parses: fakes}

      state.tx_buffer != [] ->
        case ShardRouter.extract_shard(sql) do
          {:ok, shard} ->
            new_state = flush_tx_buffer(shard, state)
            if new_state.tx_socket != nil do
              {new_stmt_map, new_entry, action} = send_parse(raw, sql, shard, new_state.tx_socket, new_state.tx_entry, new_state.stmt_map)
              fakes = new_state.pending_fake_parses + if(action == :skipped, do: 1, else: 0)
              %{new_state | stmt_map: new_stmt_map, tx_entry: new_entry, pending_fake_parses: fakes}
            else
              new_state
            end

          :no_shard ->
            client_name = extract_stmt_name(raw)
            send_fake_parse_complete(state.client_socket)
            %{state | tx_buffer: state.tx_buffer ++ [raw],
                      stmt_map: Map.put(state.stmt_map, client_name, {client_name, nil})}
        end

      begin?(sql) ->
        send_fake_parse_complete(state.client_socket)
        %{state | tx_buffer: [raw]}

      true ->
        case ShardRouter.extract_shard(sql) do
          {:ok, shard} ->
            case Pool.checkout(shard) do
              {:ok, entry} ->
                {socket, _pc, _ss} = entry
                {new_stmt_map, new_entry, action} = send_parse(raw, sql, shard, socket, entry, state.stmt_map)
                fakes = state.pending_fake_parses + if(action == :skipped, do: 1, else: 0)
                %{state | ext_socket: socket, ext_shard: shard,
                          ext_entry: new_entry, stmt_map: new_stmt_map,
                          pending_fake_parses: fakes}
              {:error, :empty} ->
                Logger.error("[Session] Pool empty for #{shard}")
                send_error(state.client_socket, "pool exhausted for shard #{shard}")
                state
            end
          :no_shard ->
            send_error(state.client_socket, "cannot route: no shard reference in Parse")
            state
        end
    end
  end

  # Send Parse to backend (or skip if already in stmt_set).
  # Returns {stmt_map, entry, :sent | :skipped}.
  # Does NOT read any response — caller must read at Sync time.
  defp send_parse(raw, sql, shard, socket, {_sock, parse_count, stmt_set}, stmt_map) do
    client_name = extract_stmt_name(raw)
    {internal_name, _} = StmtCache.get_or_create(sql)

    {new_count, new_set, action} =
      if MapSet.member?(stmt_set, internal_name) do
        {parse_count, stmt_set, :skipped}
      else
        rewritten = rewrite_parse_name(raw, client_name, internal_name)
        :ok = :gen_tcp.send(socket, rewritten)
        {parse_count + 1, MapSet.put(stmt_set, internal_name), :sent}
      end

    new_entry = {socket, new_count, new_set}
    # store shard so Bind-without-Parse can checkout the right backend
    new_map = Map.put(stmt_map, client_name, {internal_name, shard})
    {new_map, new_entry, action}
  end

  # ── Extended: Bind ────────────────────────────────────────────────────
  # Rewrite stmt name, send to backend — NO recv.

  defp handle_bind(raw, state) do
    socket = active_ext_socket(state)
    cond do
      socket != nil ->
        rewritten = rewrite_bind_name(raw, state.stmt_map) || raw
        :ok = :gen_tcp.send(socket, rewritten)
        state

      state.tx_buffer != [] ->
        send_fake_bind_complete(state.client_socket)
        %{state | tx_buffer: state.tx_buffer ++ [raw]}

      true ->
        # Bind without a preceding Parse in this pipeline.
        # Look up shard from stmt_map, checkout socket, re-parse if needed, then send Bind.
        client_name = bind_client_stmt_name(raw)
        case Map.get(state.stmt_map, client_name) do
          {internal_name, shard} when shard != nil ->
            case Pool.checkout(shard) do
              {:ok, entry} ->
                {socket, parse_count, stmt_set} = entry
                # Re-parse on this socket if it doesn't have the stmt yet.
                # If we send Parse, backend will return ParseComplete at Sync — drain it.
                {new_count, new_set, extra_drain} =
                  if MapSet.member?(stmt_set, internal_name) do
                    {parse_count, stmt_set, 0}
                  else
                    sql = StmtCache.lookup_sql(internal_name)
                    fake_parse = build_parse(internal_name, sql)
                    :ok = :gen_tcp.send(socket, fake_parse)
                    {parse_count + 1, MapSet.put(stmt_set, internal_name), 1}
                  end
                new_entry = {socket, new_count, new_set}
                rewritten = rewrite_bind_name(raw, state.stmt_map) || raw
                :ok = :gen_tcp.send(socket, rewritten)
                drains = state.pending_drain_parses + extra_drain
                %{state | ext_socket: socket, ext_shard: shard, ext_entry: new_entry,
                          pending_drain_parses: drains}

              {:error, :empty} ->
                Logger.error("[Session] Pool empty for #{shard} on Bind")
                send_error(state.client_socket, "pool exhausted for shard #{shard}")
                state
            end

          _ ->
            Logger.warning("[Session] Bind for unknown stmt #{inspect(client_name)}, ignoring")
            state
        end
    end
  end

  # ── Extended: Describe ───────────────────────────────────────────────
  # Rewrite statement name if describing a named statement.

  defp handle_describe(<<?D, _::32, ?S, rest::binary>>, state) do
    socket = active_ext_socket(state)
    cond do
      socket != nil ->
        client_name = case :binary.split(rest, <<0>>) do
          [n | _] -> n
          _ -> ""
        end
        internal = case Map.get(state.stmt_map, client_name) do
          {name, _} -> name
          nil -> client_name
        end
        body = <<?S>> <> internal <> <<0>>
        rewritten = <<?D, (byte_size(body) + 4)::32, body::binary>>
        :ok = :gen_tcp.send(socket, rewritten)
        state
      state.tx_buffer != [] ->
        send_fake_ext_response(<<?D, 0, 0, 0, 0>>, state.client_socket)
        raw_d = <<?D, (byte_size(rest) + 5)::32, ?S, rest::binary>>
        %{state | tx_buffer: state.tx_buffer ++ [raw_d]}
      true ->
        Logger.warning("[Session] Describe with no socket, ignoring")
        state
    end
  end

  defp handle_describe(raw, state), do: handle_forward_ext(raw, state)

  # ── Extended: Close ───────────────────────────────────────────────────
  # Rewrite statement name if closing a named statement.

  defp handle_close(<<?C, _::32, ?S, rest::binary>>, state) do
    socket = active_ext_socket(state)
    if socket != nil do
      client_name = case :binary.split(rest, <<0>>) do
        [n | _] -> n
        _ -> ""
      end
      internal = case Map.get(state.stmt_map, client_name) do
        {name, _} -> name
        nil -> client_name
      end
      body = <<?S>> <> internal <> <<0>>
      rewritten = <<?C, (byte_size(body) + 4)::32, body::binary>>
      :ok = :gen_tcp.send(socket, rewritten)
    end
    state
  end

  defp handle_close(raw, state), do: handle_forward_ext(raw, state)

  # ── Extended: Execute / generic forward ──────────────────────────────
  # Forward to backend — NO recv.

  defp handle_forward_ext(raw, state) do
    socket = active_ext_socket(state)
    cond do
      socket != nil ->
        :ok = :gen_tcp.send(socket, raw)
        state
      state.tx_buffer != [] ->
        send_fake_ext_response(raw, state.client_socket)
        %{state | tx_buffer: state.tx_buffer ++ [raw]}
      true ->
        Logger.warning("[Session] Ext message with no socket, ignoring")
        state
    end
  end

  # ── Extended: Flush ───────────────────────────────────────────────────
  # Flush causes backend to flush pending output — no ReadyForQuery follows.
  # After Parse+Describe+Flush the backend sends:
  #   ParseComplete(1) → ParameterDescription(t) → RowDescription(T) | NoData(n) | Error(E)
  # We forward until one of those terminals.

  @flush_terminators [?T, ?n, ?E]

  defp handle_flush(raw, state) do
    cond do
      state.tx_socket != nil ->
        # Inside a transaction — flush but keep tx_socket locked
        :ok = :gen_tcp.send(state.tx_socket, raw)
        forward_flush_response(state.tx_socket, state.client_socket)
        state

      state.ext_socket != nil ->
        # Parse+Describe+Flush pipeline (asyncpg prepare) — forward, then checkin
        :ok = :gen_tcp.send(state.ext_socket, raw)
        forward_flush_response(state.ext_socket, state.client_socket)
        Pool.checkin(state.ext_shard, state.ext_entry)
        %{state | ext_socket: nil, ext_shard: nil, ext_entry: nil}

      true ->
        state
    end
  end

  defp forward_flush_response(backend, client) do
    case recv_one_msg(backend) do
      {:ok, <<type, _::binary>> = packet} ->
        :ok = :gen_tcp.send(client, packet)
        if type in @flush_terminators, do: :ok, else: forward_flush_response(backend, client)
      {:error, reason} ->
        Logger.error("[Session] Backend recv (flush): #{inspect(reason)}")
    end
  end

  # ── Extended: Sync ────────────────────────────────────────────────────
  # Send Sync → read ALL responses (ParseComplete+BindComplete+...+ReadyForQuery) → forward.

  defp handle_sync(raw, state) do
    cond do
      state.tx_socket != nil ->
        inject_fake_parses(state.client_socket, state.pending_fake_parses)
        :ok = :gen_tcp.send(state.tx_socket, raw)
        drain_n_parse_completes(state.tx_socket, state.pending_drain_parses)
        {:ok, status} = forward_response(state.tx_socket, state.client_socket)
        if status == :idle do
          Logger.info("[Session] TX ended via Sync, checking in socket")
          Pool.checkin(state.tx_shard, state.tx_entry)
          %{state | tx_socket: nil, tx_shard: nil, tx_buffer: [], tx_entry: nil,
                    pending_fake_parses: 0, pending_drain_parses: 0}
        else
          %{state | pending_fake_parses: 0, pending_drain_parses: 0}
        end

      state.ext_socket != nil ->
        inject_fake_parses(state.client_socket, state.pending_fake_parses)
        :ok = :gen_tcp.send(state.ext_socket, raw)
        drain_n_parse_completes(state.ext_socket, state.pending_drain_parses)
        {:ok, _status} = forward_response(state.ext_socket, state.client_socket)
        Pool.checkin(state.ext_shard, state.ext_entry)
        %{state | ext_socket: nil, ext_shard: nil, ext_entry: nil,
                  pending_fake_parses: 0, pending_drain_parses: 0}

      state.tx_buffer != [] ->
        send_ready_for_query(state.client_socket, :in_transaction)
        %{state | tx_buffer: state.tx_buffer ++ [raw]}

      true ->
        send_ready_for_query(state.client_socket, :idle)
        state
    end
  end

  # ── TX buffer flush ───────────────────────────────────────────────────

  defp flush_tx_buffer(shard, state) do
    case Pool.checkout(shard) do
      {:ok, entry} ->
        {socket, _pc, _ss} = entry
        n = length(state.tx_buffer)
        Logger.info("[Session] TX checkout on #{shard}, flushing #{n} buffered packet(s)")
        Enum.each(state.tx_buffer, &:gen_tcp.send(socket, &1))
        Enum.each(1..n, fn _ -> drain_response(socket) end)
        %{state | tx_socket: socket, tx_shard: shard, tx_buffer: [], tx_entry: entry}
      {:error, :empty} ->
        Logger.error("[Session] Pool empty for #{shard} during TX flush")
        send_error(state.client_socket, "pool exhausted for shard #{shard}")
        %{state | tx_buffer: []}
    end
  end

  # ── Response forwarding ───────────────────────────────────────────────

  # Forward messages one at a time until ReadyForQuery (inclusive). Returns {:ok, tx_status}.
  defp forward_response(backend, client) do
    case recv_one_msg(backend) do
      {:ok, <<?Z, _::32, status::8>> = packet} ->
        :ok = :gen_tcp.send(client, packet)
        tx = case status do
          ?I -> :idle
          ?T -> :in_transaction
          ?E -> :failed
          _  -> :idle
        end
        {:ok, tx}
      {:ok, packet} ->
        :ok = :gen_tcp.send(client, packet)
        forward_response(backend, client)
      {:error, reason} ->
        Logger.error("[Session] Backend recv error: #{inspect(reason)}")
        send_error(client, "backend read error")
        {:ok, :idle}
    end
  end

  # Forward packets until ReadyForQuery is seen, but do NOT forward ReadyForQuery.
  # Used for Flush responses.

  defp drain_response(socket) do
    case recv_one_msg(socket) do
      {:ok, <<?Z, _::binary>>} -> :ok
      {:ok, _}                 -> drain_response(socket)
      {:error, reason}         -> Logger.error("[Session] Drain error: #{inspect(reason)}")
    end
  end

  # ── Stmt name rewrite ─────────────────────────────────────────────────

  defp rewrite_parse_name(<<?P, _::32, rest::binary>>, _old, new_name) do
    case :binary.split(rest, <<0>>) do
      [_, after_null] ->
        body = new_name <> <<0>> <> after_null
        <<?P, (byte_size(body) + 4)::32, body::binary>>
      _ -> rest
    end
  end

  defp rewrite_bind_name(<<?B, _::32, rest::binary>>, stmt_map) do
    case :binary.split(rest, <<0>>) do
      [portal, after_portal] ->
        case :binary.split(after_portal, <<0>>) do
          [client_name, after_stmt] ->
            internal = case Map.get(stmt_map, client_name) do
              {name, _shard} -> name
              nil -> client_name
            end
            body = portal <> <<0>> <> internal <> <<0>> <> after_stmt
            <<?B, (byte_size(body) + 4)::32, body::binary>>
          _ -> nil
        end
      _ -> nil
    end
  end

  defp bind_client_stmt_name(<<?B, _::32, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [_portal, after_portal] ->
        case :binary.split(after_portal, <<0>>) do
          [client_name | _] -> client_name
          _ -> nil
        end
      _ -> nil
    end
  end

  # Build a minimal Parse message: stmt_name, sql, 0 params
  defp build_parse(stmt_name, sql) when is_binary(sql) do
    body = stmt_name <> <<0>> <> sql <> <<0, 0, 0>>
    <<?P, (byte_size(body) + 4)::32, body::binary>>
  end

  defp extract_stmt_name(<<?P, _::32, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [name | _] -> name
      _ -> ""
    end
  end

  defp extract_stmt_name(_), do: ""

  # ── Helpers ───────────────────────────────────────────────────────────

  defp active_ext_socket(%{tx_socket: s}) when s != nil, do: s
  defp active_ext_socket(%{ext_socket: s}), do: s

  defp begin?(sql), do: Regex.match?(~r/^\s*BEGIN\b/i, sql)

  defp cleanup(%{tx_socket: s, tx_shard: sh, tx_entry: e}) when s != nil, do: Pool.checkin(sh, e)
  defp cleanup(%{ext_socket: s, ext_shard: sh, ext_entry: e}) when s != nil, do: Pool.checkin(sh, e)
  defp cleanup(_), do: :ok

  # Drain exactly N ParseComplete ('1') messages using proper message framing.
  defp drain_n_parse_completes(_socket, 0), do: :ok
  defp drain_n_parse_completes(socket, n) when n > 0 do
    case recv_one_msg(socket) do
      {:ok, <<?1, _::binary>>} ->
        drain_n_parse_completes(socket, n - 1)
      {:ok, packet} ->
        Logger.warning("[Session] drain_n_parse_completes: unexpected type=0x#{Integer.to_string(:binary.first(packet), 16)}")
      {:error, reason} ->
        Logger.error("[Session] drain_n_parse_completes error: #{inspect(reason)}")
    end
  end

  # Read exactly one PG message (type + len + body).
  defp recv_one_msg(socket) do
    with {:ok, <<type, len::32>>} <- :gen_tcp.recv(socket, 5, 5_000) do
      body_len = len - 4
      if body_len > 0 do
        case :gen_tcp.recv(socket, body_len, 5_000) do
          {:ok, body} -> {:ok, <<type, len::32, body::binary>>}
          err -> err
        end
      else
        {:ok, <<type, len::32>>}
      end
    end
  end

  defp inject_fake_parses(_socket, 0), do: :ok
  defp inject_fake_parses(socket, n) when n > 0 do
    :gen_tcp.send(socket, <<?1, 4::32>>)
    inject_fake_parses(socket, n - 1)
  end

  defp send_fake_parse_complete(socket),
    do: :gen_tcp.send(socket, <<?1, 4::32>>)

  defp send_fake_bind_complete(socket),
    do: :gen_tcp.send(socket, <<?2, 4::32>>)

  defp send_fake_ext_response(<<?E, _::binary>>, socket),
    do: send_command_complete(socket, "BEGIN")
  defp send_fake_ext_response(<<?C, _::binary>>, socket),
    do: :gen_tcp.send(socket, <<?3, 4::32>>)
  defp send_fake_ext_response(<<?D, _::binary>>, socket),
    do: :gen_tcp.send(socket, <<?n, 4::32>>)
  defp send_fake_ext_response(_, _), do: :ok

  defp send_auth_ok(socket) do
    params = [
      {"server_version", "14.0"},
      {"server_encoding", "UTF8"},
      {"client_encoding", "UTF8"},
      {"DateStyle", "ISO, MDY"},
      {"integer_datetimes", "on"},
      {"TimeZone", "UTC"}
    ]
    param_msgs = Enum.map(params, fn {k, v} ->
      body = k <> <<0>> <> v <> <<0>>
      <<?S, (byte_size(body) + 4)::32, body::binary>>
    end)
    # BackendKeyData (K) — pid and secret key, dummy values
    backend_key = <<?K, 12::32, 0::32, 0::32>>
    :gen_tcp.send(socket, [<<?R, 8::32, 0::32>>, param_msgs, backend_key, <<?Z, 5::32, ?I>>])
  end

  defp send_command_complete(socket, tag) do
    body = tag <> <<0>>
    :gen_tcp.send(socket, <<?C, (byte_size(body) + 4)::32, body::binary>>)
  end

  defp send_ready_for_query(socket, status) do
    byte = case status do
      :idle           -> ?I
      :in_transaction -> ?T
      :failed         -> ?E
    end
    :gen_tcp.send(socket, <<?Z, 5::32, byte>>)
  end

  defp send_error(socket, msg) do
    body = <<?S, "ERROR", 0, ?M, msg::binary, 0, 0>>
    :gen_tcp.send(socket, <<?E, (byte_size(body) + 4)::32, body::binary>>)
    send_ready_for_query(socket, :idle)
  end
end
