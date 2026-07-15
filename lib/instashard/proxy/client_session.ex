defmodule Instashard.Proxy.ClientSession.Msg do
  @moduledoc false
  defstruct raw: nil, drain_backend: false
end

defmodule Instashard.Proxy.ClientSession do
  @moduledoc """
  Simplified ClientSession: unified buffer + single drain loop.

  Pre-processing (before buffer):
    - Q("BEGIN")       → fake CC+RFQ to client, buffer with drain_backend: true
    - P(_, "BEGIN")    → fake ParseComplete to client, buffer with drain_backend: true
    - P(_, sql)        → StmtCache rewrite (internal_name), extract shard → stmt_map, buffer
    - B                → rewrite stmt name in raw, buffer
    - D/C              → rewrite stmt name in raw, buffer
    - everything else  → buffer as-is

  Drain loop (drain_buffer/1):
    No conn:
      scan buffer for first shard-bearing msg (Q sql / P sql / B→stmt_map)
      → checkout → conn assigned → drain again
      → :empty/:migrating → waiting = true, stop

    Has conn:
      Q, S         → send + read responses until RFQ → checkin on RFQ('I'/'E')
      P            → check stmt_set; skip (fake_parses++) or send + update stmt_set
      B            → check stmt_set; re-parse inline if needed (drain_parses++), send
      H            → send + read until flush terminator
      everything else → send only

  checkin is driven solely by backend RFQ status byte.
  """

  use GenServer
  require Logger

  alias __MODULE__.Msg
  alias Instashard.Backend.{Manager, Pool, StmtCache}
  alias Instashard.Proxy.{PgProtocol, ShardRouter}

  defstruct [
    client_socket: nil,
    # nil | %{socket, db_id, shard, entry}
    conn: nil,
    buffer: :queue.new(),
    waiting: false,
    waiting_shard: nil,
    retry_attempt: 0,
    # client_name → {internal_name, shard | nil}
    stmt_map: %{},
    pending_fake_parses: 0,
    pending_drain_parses: 0,
    partial: <<>>
  ]

  def start_link(client_socket), do: GenServer.start_link(__MODULE__, client_socket)

  @impl true
  def init(client_socket) do
    {:ok, %__MODULE__{client_socket: client_socket}}
  end

  @impl true
  def terminate(:normal, _state), do: :ok
  def terminate(:shutdown, _state), do: :ok
  def terminate({:shutdown, _}, _state), do: :ok
  def terminate(_reason, _state), do: :ok

  # ── TCP / control events ──────────────────────────────────────────────

  @impl true
  def handle_info(:socket_ready, state) do
    :inet.setopts(state.client_socket, active: true)
    {:noreply, state}
  end

  def handle_info({:tcp, _sock, data}, state) do
    state = split_and_enqueue(state.partial <> data, %{state | partial: <<>>})
    {:noreply, if(state.waiting, do: state, else: drain_buffer(state))}
  end

  def handle_info({:gate_open, shard}, %{waiting_shard: shard} = state) do
    Logger.info("[Session] #{inspect(self())} gate:#{shard} opened, resuming")
    Phoenix.PubSub.unsubscribe(Instashard.PubSub, "gate:#{shard}")
    state = %{state | waiting: false, waiting_shard: nil, retry_attempt: 0}
    {:noreply, drain_buffer(state)}
  end

  def handle_info({:gate_open, _}, state), do: {:noreply, state}

  def handle_info({:retry_checkout, attempt}, state) do
    state = %{state | waiting: false, retry_attempt: attempt}
    {:noreply, drain_buffer(state)}
  end

  def handle_info({:tcp_closed, _}, state) do
    cleanup(state)
    {:stop, :normal, state}
  end

  def handle_info({:tcp_error, _, reason}, state) do
    Logger.error("[Session] TCP error: #{inspect(reason)}")
    cleanup(state)
    {:stop, :normal, state}
  end

  # ── Split TCP segment → PG messages → enqueue ────────────────────────

  defp split_and_enqueue(data, state), do: do_split(data, state)

  # SSL negotiation
  defp do_split(<<0, 0, 0, 8, 4, 210, 22, 47, rest::binary>>, state) do
    :gen_tcp.send(state.client_socket, "N")
    do_split(rest, state)
  end

  # Startup message (length-prefixed, no type byte)
  defp do_split(<<len::32, 0, 3, 0, 0, _::binary>> = data, state) when byte_size(data) >= len do
    <<msg::binary-size(len), rest::binary>> = data
    <<_len::32, 0, 3, 0, 0, payload::binary>> = msg
    params = PgProtocol.parse_startup_params(payload)
    Logger.info("[Session] Startup user=#{params["user"]} db=#{params["database"]}")
    send_auth_ok(state.client_socket)
    do_split(rest, state)
  end

  # Normal PG message: type(1) + len(4, includes itself) + body
  defp do_split(<<type, len::32, rest::binary>>, state) when len >= 4 do
    body_len = len - 4
    case rest do
      <<body::binary-size(body_len), remaining::binary>> ->
        state = preprocess_and_enqueue(<<type, len::32, body::binary>>, state)
        do_split(remaining, state)
      _ ->
        %{state | partial: <<type, len::32, rest::binary>>}
    end
  end

  defp do_split(<<>>, state), do: state
  defp do_split(data, state), do: %{state | partial: data}

  # ── Pre-process + enqueue ─────────────────────────────────────────────

  # Client disconnect
  defp preprocess_and_enqueue(<<?X, _::32>>, state) do
    cleanup(state)
    state
  end

  # Simple query
  defp preprocess_and_enqueue(<<?Q, size::32, rest::binary>> = raw, state) do
    sql_len = size - 4 - 1
    <<sql::binary-size(sql_len), _::8>> = rest
    if begin?(sql) do
      send_command_complete(state.client_socket, "BEGIN")
      send_ready_for_query(state.client_socket, :in_transaction)
      enq(state, %Msg{raw: raw, drain_backend: true})
    else
      enq(state, %Msg{raw: raw})
    end
  end

  # Parse — rewrite stmt name, record shard in stmt_map
  defp preprocess_and_enqueue(<<?P, _::32, _::binary>> = raw, state) do
    sql = PgProtocol.parse_sql_from_parse(raw)
    client_name = extract_stmt_name(raw)

    if begin?(sql) do
      send_fake_parse_complete(state.client_socket)
      new_map = Map.put(state.stmt_map, client_name, {client_name, nil})
      enq(%{state | stmt_map: new_map, pending_drain_parses: state.pending_drain_parses + 1},
          %Msg{raw: raw, drain_backend: true})
    else
      {internal_name, _} = StmtCache.get_or_create(sql)
      shard = case ShardRouter.extract_shard(sql) do
        {:ok, s} -> s
        :no_shard -> nil
      end
      new_map = Map.put(state.stmt_map, client_name, {internal_name, shard})
      rewritten = rewrite_parse_name(raw, client_name, internal_name)
      enq(%{state | stmt_map: new_map}, %Msg{raw: rewritten})
    end
  end

  # Bind — rewrite stmt name
  defp preprocess_and_enqueue(<<?B, _::32, _::binary>> = raw, state) do
    rewritten = rewrite_bind_name(raw, state.stmt_map) || raw
    enq(state, %Msg{raw: rewritten})
  end

  # Describe statement — rewrite name
  defp preprocess_and_enqueue(<<?D, _::32, ?S, rest::binary>>, state) do
    client_name = case :binary.split(rest, <<0>>) do
      [n | _] -> n
      _ -> ""
    end
    internal = case Map.get(state.stmt_map, client_name) do
      {name, _} -> name
      nil -> client_name
    end
    body = <<?S>> <> internal <> <<0>>
    enq(state, %Msg{raw: <<?D, (byte_size(body) + 4)::32, body::binary>>})
  end

  # Close statement — rewrite name
  defp preprocess_and_enqueue(<<?C, _::32, ?S, rest::binary>>, state) do
    client_name = case :binary.split(rest, <<0>>) do
      [n | _] -> n
      _ -> ""
    end
    internal = case Map.get(state.stmt_map, client_name) do
      {name, _} -> name
      nil -> client_name
    end
    body = <<?S>> <> internal <> <<0>>
    enq(state, %Msg{raw: <<?C, (byte_size(body) + 4)::32, body::binary>>})
  end

  # Everything else (E, H, S, D portal, C portal, etc.)
  defp preprocess_and_enqueue(raw, state), do: enq(state, %Msg{raw: raw})

  defp enq(state, msg), do: %{state | buffer: :queue.in(msg, state.buffer)}

  # ── Drain loop ────────────────────────────────────────────────────────

  defp drain_buffer(state) do
    if :queue.is_empty(state.buffer) do
      state
    else
      do_drain(state)
    end
  end

  # No connection — find shard and checkout
  defp do_drain(%{conn: nil} = state) do
    case find_shard(state) do
      {:ok, shard} -> do_checkout(shard, state)
      :no_shard    -> state
    end
  end

  # Have connection — collect send-only messages into a pending iolist,
  # then flush everything in one send when we hit Q/S/H (send+recv messages).
  # This reduces syscalls from N per extended-query cycle to 1.
  defp do_drain(%{conn: _} = state), do: drain_collect(state, [])

  # Collect loop: accumulate send-only messages, flush on send+recv triggers.
  defp drain_collect(%{conn: conn} = state, pending) do
    if :queue.is_empty(state.buffer) do
      flush_pending(conn, pending)
      state
    else
      {{:value, %Msg{raw: raw, drain_backend: drain?}}, rest} = :queue.out(state.buffer)
      state = %{state | buffer: rest}
      {socket, _pc, _ss} = conn.entry
      type = if raw, do: :binary.first(raw), else: nil

      cond do
        # Simple query or Sync: flush pending + this msg together, then recv
        type in [?Q, ?S] ->
          payload = if raw, do: Enum.reverse([raw | pending]), else: Enum.reverse(pending)
          send_result = if payload == [], do: :ok, else: :gen_tcp.send(socket, payload)
          case send_result do
            :ok ->
              skip_parses = if(type == ?S, do: state.pending_drain_parses, else: 0)
              inject = if(type == ?S, do: state.pending_fake_parses, else: 0)
              state = %{state | pending_fake_parses: 0, pending_drain_parses: 0}
              {rfq_status, state} = read_until_rfq(socket, state, drain?, skip_parses, inject)
              after_rfq(rfq_status, state)
            {:error, reason} ->
              handle_backend_error(reason, state)
          end

        # Flush: flush pending + H together, then recv flush response
        type == ?H ->
          payload = Enum.reverse([raw | pending])
          case :gen_tcp.send(socket, payload) do
            :ok ->
              case forward_flush_response(socket, state.client_socket) do
                :ok -> drain_collect(state, [])
                {:error, reason} -> handle_backend_error(reason, state)
              end
            {:error, reason} -> handle_backend_error(reason, state)
          end

        # Parse: skip if stmt already on this socket, else accumulate
        type == ?P ->
          internal_name = extract_stmt_name(raw)
          {_sock, parse_count, stmt_set} = conn.entry
          if MapSet.member?(stmt_set, internal_name) do
            drain_collect(%{state | pending_fake_parses: state.pending_fake_parses + 1}, pending)
          else
            new_entry = {socket, parse_count + 1, MapSet.put(stmt_set, internal_name)}
            drain_collect(%{state | conn: %{conn | entry: new_entry}}, [raw | pending])
          end

        # Bind: ensure stmt is on socket (inject re-parse if needed), accumulate
        type == ?B ->
          internal_name = bind_stmt_name(raw)
          {_sock, parse_count, stmt_set} = conn.entry
          if internal_name && !MapSet.member?(stmt_set, internal_name) do
            sql = StmtCache.lookup_sql(internal_name)
            parse_msg = build_parse(internal_name, sql)
            new_entry = {socket, parse_count + 1, MapSet.put(stmt_set, internal_name)}
            drain_collect(
              %{state | conn: %{conn | entry: new_entry},
                        pending_drain_parses: state.pending_drain_parses + 1},
              [raw, parse_msg | pending])
          else
            drain_collect(state, [raw | pending])
          end

        # nil raw (internal placeholder) — skip
        raw == nil ->
          drain_collect(state, pending)

        # E, D, C and anything else: accumulate
        true ->
          drain_collect(state, [raw | pending])
      end
    end
  end

  # Send any accumulated but unflushed messages (e.g. buffer emptied without a Sync).
  defp flush_pending(_conn, []), do: :ok
  defp flush_pending(conn, pending) do
    {socket, _pc, _ss} = conn.entry
    :gen_tcp.send(socket, Enum.reverse(pending))
  end

  # conn is nil when read_until_rfq already called handle_backend_error
  defp after_rfq(_, %{conn: nil} = state), do: drain_buffer(state)

  defp after_rfq(:idle, state) do
    Pool.checkin(state.conn.db_id, state.conn.shard, state.conn.entry)
    drain_buffer(%{state | conn: nil})
  end

  # Backend is in an aborted transaction block — discard rather than checkin,
  # otherwise the next checkout inherits a connection that errors on every query.
  defp after_rfq(:failed, state) do
    {socket, _pc, _ss} = state.conn.entry
    Pool.decrement_tx(state.conn.shard)
    Manager.discard(state.conn.db_id, socket)
    drain_buffer(%{state | conn: nil})
  end

  defp after_rfq(:in_transaction, state) do
    drain_buffer(state)
  end

  # ── Checkout ──────────────────────────────────────────────────────────

  defp do_checkout(shard, state) do
    case Pool.checkout(shard) do
      {:ok, db_id, entry} ->
        {socket, _pc, _ss} = entry
        conn = %{socket: socket, db_id: db_id, shard: shard, entry: entry}
        new_map = Map.new(state.stmt_map, fn
          {k, {internal, nil}} -> {k, {internal, shard}}
          pair -> pair
        end)
        drain_buffer(%{state | conn: conn, retry_attempt: 0, stmt_map: new_map})

      {:error, :migrating} ->
        Logger.info("[Session] #{inspect(self())} shard #{shard} gate closed, subscribing gate:#{shard}")
        Phoenix.PubSub.subscribe(Instashard.PubSub, "gate:#{shard}")
        %{state | waiting: true, waiting_shard: shard}

      {:error, :empty} ->
        schedule_retry(shard, state)

      {:error, :no_mapping} ->
        send_error(state.client_socket, "no db mapping for shard #{shard}")
        %{state | buffer: :queue.new(), waiting: false}
    end
  end

  @max_retry_attempts 6

  defp schedule_retry(shard, state) do
    next = state.retry_attempt + 1
    if next <= @max_retry_attempts do
      delay = trunc(:math.pow(2, next - 1))
      Process.send_after(self(), {:retry_checkout, next}, delay)
      %{state | waiting: true, waiting_shard: shard, retry_attempt: next}
    else
      Logger.error("[Session] Pool exhausted for #{shard} after #{@max_retry_attempts} retries")
      send_error(state.client_socket, "pool exhausted for shard #{shard}")
      %{state | buffer: :queue.new(), waiting: false, retry_attempt: 0}
    end
  end

  # ── Find shard in buffer ──────────────────────────────────────────────

  defp find_shard(%{buffer: buffer, stmt_map: stmt_map}) do
    buffer
    |> :queue.to_list()
    |> Enum.find_value(:no_shard, fn %Msg{raw: raw} ->
      with true <- is_binary(raw),
           type = :binary.first(raw),
           true <- type in [?Q, ?P, ?B] do
        case type do
          ?Q ->
            sql = extract_sql_from_query(raw)
            case ShardRouter.extract_shard(sql) do
              {:ok, shard} -> {:ok, shard}
              :no_shard    -> nil
            end
          ?P ->
            sql = PgProtocol.parse_sql_from_parse(raw)
            case ShardRouter.extract_shard(sql) do
              {:ok, shard} -> {:ok, shard}
              :no_shard    -> nil
            end
          ?B ->
            # After preprocess, the Bind message contains the internal_name.
            # Try client_name lookup first, then search by internal_name in values.
            name = bind_stmt_name(raw)
            case Map.get(stmt_map, name) do
              {_internal, shard} when shard != nil -> {:ok, shard}
              _ -> find_shard_by_internal(stmt_map, name)
            end
        end
      else
        _ -> nil
      end
    end)
  end

  defp find_shard_by_internal(stmt_map, internal_name) do
    Enum.find_value(stmt_map, nil, fn
      {_, {^internal_name, shard}} when shard != nil -> {:ok, shard}
      _ -> nil
    end)
  end

  # ── Response reading ──────────────────────────────────────────────────

  # Reads the full backend response for one Query/Sync in as few recv/send
  # syscalls as possible: bulk-recv whatever the kernel has buffered, scan
  # it in memory for complete PG messages (length-prefixed, so a stray `Z`
  # byte inside a DataRow can never be mistaken for ReadyForQuery), and
  # forward everything accumulated so far in a single send once RFQ is seen.
  #
  # skip_parses: number of ParseComplete ('1') messages from drain-parses to
  #   silently drop (they were sent by us, not the client).
  # inject_parses: number of fake ParseComplete messages to inject into the
  #   response stream (for parses we skipped sending to the backend).
  defp read_until_rfq(socket, state, drain?, skip_parses, inject_parses) do
    case read_until_rfq_loop(socket, <<>>, [], skip_parses) do
      {:ok, status, iolist} ->
        unless drain? do
          if inject_parses > 0 do
            fake = :binary.copy(<<?1, 4::32>>, inject_parses)
            :gen_tcp.send(state.client_socket, [fake | iolist])
          else
            :gen_tcp.send(state.client_socket, iolist)
          end
        end
        {status, state}

      {:error, reason} ->
        Logger.error("[Session] Backend recv error: #{inspect(reason)}")
        {:idle, handle_backend_error(reason, state)}
    end
  end

  defp read_until_rfq_loop(socket, partial, acc, skip) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} ->
        case scan_rfq_messages(partial <> data, acc, skip) do
          {:done, status, iolist} -> {:ok, status, iolist}
          {:continue, new_acc, rest, new_skip} -> read_until_rfq_loop(socket, rest, new_acc, new_skip)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp scan_rfq_messages(<<?Z, 5::32, status::8, remaining::binary>>, acc, _skip) do
    if byte_size(remaining) > 0 do
      Logger.warning("[Session] #{byte_size(remaining)} unexpected bytes trailing RFQ, discarded")
    end
    {:done, rfq_status(status), Enum.reverse([<<?Z, 5::32, status::8>> | acc])}
  end

  defp scan_rfq_messages(<<?1, 4::32, remaining::binary>>, acc, skip) when skip > 0 do
    scan_rfq_messages(remaining, acc, skip - 1)
  end

  defp scan_rfq_messages(<<type, len::32, rest::binary>> = data, acc, skip) when len >= 4 do
    body_len = len - 4
    case rest do
      <<body::binary-size(body_len), remaining::binary>> ->
        scan_rfq_messages(remaining, [<<type, len::32, body::binary>> | acc], skip)
      _ ->
        {:continue, acc, data, skip}
    end
  end

  defp scan_rfq_messages(data, acc, skip), do: {:continue, acc, data, skip}

  defp rfq_status(?I), do: :idle
  defp rfq_status(?T), do: :in_transaction
  defp rfq_status(?E), do: :failed
  defp rfq_status(_), do: :idle

  # ── Flush response: batch recv + scan for terminator ──────────────────

  @flush_terminators [?T, ?n, ?E, ?1]

  defp forward_flush_response(backend, client) do
    case read_until_flush_terminator(backend, <<>>, []) do
      {:ok, iolist} -> :gen_tcp.send(client, iolist)
      {:error, reason} ->
        Logger.error("[Session] Flush recv: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp read_until_flush_terminator(socket, partial, acc) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} ->
        case scan_flush_messages(partial <> data, acc) do
          {:done, iolist} -> {:ok, iolist}
          {:continue, new_acc, rest} -> read_until_flush_terminator(socket, rest, new_acc)
        end
      {:error, reason} -> {:error, reason}
    end
  end

  defp scan_flush_messages(<<type, len::32, rest::binary>> = data, acc) when len >= 4 do
    body_len = len - 4
    case rest do
      <<body::binary-size(body_len), remaining::binary>> ->
        msg = <<type, len::32, body::binary>>
        if type in @flush_terminators do
          {:done, Enum.reverse([msg | acc])}
        else
          scan_flush_messages(remaining, [msg | acc])
        end
      _ ->
        {:continue, acc, data}
    end
  end

  defp scan_flush_messages(data, acc), do: {:continue, acc, data}

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
              {name, _} -> name
              nil -> client_name
            end
            body = portal <> <<0>> <> internal <> <<0>> <> after_stmt
            <<?B, (byte_size(body) + 4)::32, body::binary>>
          _ -> nil
        end
      _ -> nil
    end
  end

  defp bind_stmt_name(<<?B, _::32, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [_portal, after_portal] ->
        case :binary.split(after_portal, <<0>>) do
          [name | _] -> name
          _ -> nil
        end
      _ -> nil
    end
  end


  defp extract_stmt_name(<<?P, _::32, rest::binary>>) do
    case :binary.split(rest, <<0>>) do
      [name | _] -> name
      _ -> ""
    end
  end

  defp extract_stmt_name(_), do: ""

  defp extract_sql_from_query(<<?Q, size::32, rest::binary>>) do
    sql_len = size - 4 - 1
    <<sql::binary-size(sql_len), _::8>> = rest
    sql
  end

  defp build_parse(stmt_name, sql) when is_binary(sql) do
    body = stmt_name <> <<0>> <> sql <> <<0, 0, 0>>
    <<?P, (byte_size(body) + 4)::32, body::binary>>
  end

  # ── Misc ──────────────────────────────────────────────────────────────

  defp begin?(sql) do
    case String.trim_leading(sql) do
      <<b, e, g, i, n, rest::binary>>
      when b in ~c[Bb] and e in ~c[Ee] and g in ~c[Gg] and i in ~c[Ii] and n in ~c[Nn] ->
        rest == "" or not word_char?(:binary.first(rest))
      _ -> false
    end
  end

  defp word_char?(c) when c in ?a..?z, do: true
  defp word_char?(c) when c in ?A..?Z, do: true
  defp word_char?(c) when c in ?0..?9, do: true
  defp word_char?(?_), do: true
  defp word_char?(_), do: false

  defp cleanup(%{conn: %{db_id: db, shard: sh, entry: e}}), do: Pool.checkin(db, sh, e)
  defp cleanup(_), do: :ok

  defp handle_backend_error(reason, state) do
    Logger.error("[Session] Backend error: #{inspect(reason)}")
    if state.conn do
      Pool.decrement_tx(state.conn.shard)
      Manager.discard(state.conn.db_id, state.conn.socket)
      send_error_08006(state.client_socket, "backend connection failure")
    end
    %{state | conn: nil, buffer: :queue.new(), pending_fake_parses: 0, pending_drain_parses: 0}
  end

  defp send_error_08006(socket, msg) do
    body = <<?S, "ERROR", 0, ?V, "ERROR", 0, ?C, "08006", 0, ?M, msg::binary, 0, 0>>
    :gen_tcp.send(socket, <<?E, (byte_size(body) + 4)::32, body::binary>>)
    send_ready_for_query(socket, :idle)
  end



  defp send_fake_parse_complete(socket), do: :gen_tcp.send(socket, <<?1, 4::32>>)

  defp send_auth_ok(socket) do
    params = [
      {"server_version", "14.0"}, {"server_encoding", "UTF8"},
      {"client_encoding", "UTF8"}, {"DateStyle", "ISO, MDY"},
      {"integer_datetimes", "on"}, {"TimeZone", "UTC"}
    ]
    param_msgs = Enum.map(params, fn {k, v} ->
      body = k <> <<0>> <> v <> <<0>>
      <<?S, (byte_size(body) + 4)::32, body::binary>>
    end)
    :gen_tcp.send(socket, [<<?R, 8::32, 0::32>>, param_msgs, <<?K, 12::32, 0::32, 0::32>>, <<?Z, 5::32, ?I>>])
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
