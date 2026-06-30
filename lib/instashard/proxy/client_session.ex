defmodule Instashard.Proxy.ClientSession do
  @moduledoc """
  One GenServer per client connection.

  Supports Simple Query and Extended Protocol (Parse/Bind/Execute/Sync).

  Transaction (lazy checkout):
    - BEGIN/Parse("BEGIN") → buffer packets, fake responses
    - First shard-bearing packet → checkout socket, flush buffer (drain responses), lock socket
    - COMMIT/ROLLBACK → forward on tx_socket, checkin on ReadyForQuery('I')

  Extended protocol non-transaction:
    - Parse → extract shard, checkout ext_socket, forward
    - Bind/Describe/Execute/Close/Flush → forward on ext_socket
    - Sync → forward on ext_socket, forward ReadyForQuery, checkin ext_socket
  """

  use GenServer
  require Logger

  alias Instashard.Backend.Pool
  alias Instashard.Proxy.PgProtocol
  alias Instashard.Proxy.ShardRouter

  # Extended protocol response terminators — these end a single request/response unit.
  # ReadyForQuery (Z) is excluded here; it is only consumed by handle_sync.
  @ext_unit_terminators [
    ?1,   # ParseComplete
    ?2,   # BindComplete
    ?3,   # CloseComplete
    ?C,   # CommandComplete
    ?I,   # EmptyQueryResponse
    ?T,   # RowDescription (terminal for Describe)
    ?n,   # NoData (terminal for Describe)
    ?s,   # PortalSuspended
    ?E    # ErrorResponse
  ]

  defstruct [
    client_socket: nil,
    tx_socket: nil,
    tx_shard: nil,
    tx_buffer: [],
    ext_socket: nil,
    ext_shard: nil
  ]

  def start_link(client_socket) do
    GenServer.start_link(__MODULE__, client_socket)
  end

  @impl true
  def init(client_socket) do
    :inet.setopts(client_socket, active: true)
    {:ok, %__MODULE__{client_socket: client_socket}}
  end

  # ── TCP events ───────────────────────────────────────────────────────

  @impl true
  def handle_info({:tcp, _sock, data}, state) do
    {:noreply, handle_packet(data, state)}
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

  # ── packet dispatch ──────────────────────────────────────────────────

  # SSL request
  defp handle_packet(<<0, 0, 0, 8, 4, 210, 22, 47>>, state) do
    :gen_tcp.send(state.client_socket, "N")
    state
  end

  # Startup
  defp handle_packet(<<_len::32, 0, 3, 0, 0, payload::binary>>, state) do
    params = PgProtocol.parse_startup_params(payload)
    Logger.info("[Session] Startup user=#{params["user"]} db=#{params["database"]}")
    send_auth_ok(state.client_socket)
    state
  end

  # Simple Query
  defp handle_packet(<<?Q, size::32, rest::binary>> = raw, state) do
    sql_len = size - 4 - 1
    <<sql::binary-size(sql_len), _::8>> = rest
    Logger.info("[Session] SimpleQuery: #{sql}")
    handle_simple_query(sql, raw, state)
  end

  # Extended: Parse
  defp handle_packet(<<?P, _::32, _::binary>> = raw, state) do
    sql = PgProtocol.parse_sql_from_parse(raw)
    Logger.info("[Session] Parse: #{sql}")
    handle_parse(sql, raw, state)
  end

  # Extended: Bind / Describe / Execute / Close / Flush
  defp handle_packet(<<?B, _::32, _::binary>> = raw, state), do: handle_ext_message(raw, state)
  defp handle_packet(<<?D, _::32, _::binary>> = raw, state), do: handle_ext_message(raw, state)
  defp handle_packet(<<?E, _::32, _::binary>> = raw, state), do: handle_ext_message(raw, state)
  defp handle_packet(<<?C, _::32, _::binary>> = raw, state), do: handle_ext_message(raw, state)
  defp handle_packet(<<?H, 4::32>>, state),                  do: handle_ext_message(<<?H, 4::32>>, state)

  # Extended: Sync — ends the pipeline, triggers ReadyForQuery
  defp handle_packet(<<?S, 4::32>> = raw, state), do: handle_sync(raw, state)

  # Terminate
  defp handle_packet(<<?X, _::32>>, state) do
    cleanup(state)
    {:stop, :normal, state}
    state
  end

  defp handle_packet(packet, state) do
    Logger.debug("[Session] Unhandled packet type=#{inspect(:binary.first(packet))}")
    state
  end

  # ── Simple Query ─────────────────────────────────────────────────────

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
          {:ok, socket} ->
            :ok = :gen_tcp.send(socket, raw)
            forward_response(socket, state.client_socket)
            Pool.checkin(shard, socket)

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
      Pool.checkin(state.tx_shard, state.tx_socket)
      %{state | tx_socket: nil, tx_shard: nil, tx_buffer: []}
    else
      state
    end
  end

  # ── Extended Protocol: Parse ──────────────────────────────────────────

  defp handle_parse(sql, raw, state) do
    cond do
      # In active transaction — forward directly
      state.tx_socket != nil ->
        :ok = :gen_tcp.send(state.tx_socket, raw)
        forward_ext_unit(state.tx_socket, state.client_socket)
        state

      # In buffered tx (BEGIN received) — first shard-bearing parse
      state.tx_buffer != [] ->
        case ShardRouter.extract_shard(sql) do
          {:ok, shard} ->
            new_state = flush_tx_buffer(shard, state)
            if new_state.tx_socket != nil do
              :ok = :gen_tcp.send(new_state.tx_socket, raw)
              forward_ext_unit(new_state.tx_socket, state.client_socket)
              new_state
            else
              new_state
            end

          :no_shard ->
            # Still in buffer phase (e.g. prepared stmt with no shard)
            send_fake_parse_complete(state.client_socket)
            %{state | tx_buffer: state.tx_buffer ++ [raw]}
        end

      # BEGIN via extended protocol
      begin?(sql) ->
        send_fake_parse_complete(state.client_socket)
        %{state | tx_buffer: [raw]}

      # Normal extended query — checkout socket
      true ->
        case ShardRouter.extract_shard(sql) do
          {:ok, shard} ->
            case Pool.checkout(shard) do
              {:ok, socket} ->
                :ok = :gen_tcp.send(socket, raw)
                forward_ext_unit(socket, state.client_socket)
                %{state | ext_socket: socket, ext_shard: shard}

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

  # ── Extended Protocol: Bind / Describe / Execute / Close / Flush ─────

  defp handle_ext_message(raw, state) do
    socket = active_ext_socket(state)

    cond do
      socket != nil ->
        :ok = :gen_tcp.send(socket, raw)
        # Flush has no direct response — skip forwarding
        unless raw == <<?H, 4::32>>,
          do: forward_ext_unit(socket, state.client_socket)
        state

      state.tx_buffer != [] ->
        # Still buffering BEGIN sequence — fake the response
        send_fake_ext_response(raw, state.client_socket)
        %{state | tx_buffer: state.tx_buffer ++ [raw]}

      true ->
        Logger.warning("[Session] Extended message with no socket, ignoring")
        state
    end
  end

  # ── Extended Protocol: Sync ───────────────────────────────────────────

  defp handle_sync(raw, state) do
    cond do
      state.tx_socket != nil ->
        :ok = :gen_tcp.send(state.tx_socket, raw)
        {:ok, status} = forward_response(state.tx_socket, state.client_socket)

        if status == :idle do
          Logger.info("[Session] TX ended via Sync, checking in socket")
          Pool.checkin(state.tx_shard, state.tx_socket)
          %{state | tx_socket: nil, tx_shard: nil, tx_buffer: []}
        else
          state
        end

      state.ext_socket != nil ->
        :ok = :gen_tcp.send(state.ext_socket, raw)
        {:ok, _status} = forward_response(state.ext_socket, state.client_socket)
        Pool.checkin(state.ext_shard, state.ext_socket)
        %{state | ext_socket: nil, ext_shard: nil}

      state.tx_buffer != [] ->
        # Sync closes a buffered BEGIN sequence (no shard appeared yet)
        send_ready_for_query(state.client_socket, :in_transaction)
        %{state | tx_buffer: state.tx_buffer ++ [raw]}

      true ->
        # No socket at all — client sent spurious Sync, just ack
        send_ready_for_query(state.client_socket, :idle)
        state
    end
  end

  # ── TX buffer flush ───────────────────────────────────────────────────

  defp flush_tx_buffer(shard, state) do
    case Pool.checkout(shard) do
      {:ok, socket} ->
        n = length(state.tx_buffer)
        Logger.info("[Session] TX checkout on #{shard}, flushing #{n} buffered packet(s)")
        Enum.each(state.tx_buffer, &:gen_tcp.send(socket, &1))
        # Client already got fake responses — drain and discard real ones.
        Enum.each(1..n, fn _ -> drain_response(socket) end)
        %{state | tx_socket: socket, tx_shard: shard, tx_buffer: []}

      {:error, :empty} ->
        Logger.error("[Session] Pool empty for #{shard} during TX flush")
        send_error(state.client_socket, "pool exhausted for shard #{shard}")
        %{state | tx_buffer: []}
    end
  end

  # ── response forwarding ───────────────────────────────────────────────

  # Forward packets until ReadyForQuery. Returns {:ok, tx_status}.
  defp forward_response(backend, client) do
    case :gen_tcp.recv(backend, 0, 5000) do
      {:ok, packet} ->
        :ok = :gen_tcp.send(client, packet)

        case PgProtocol.ready_for_query_status(packet) do
          {:ok, status} -> {:ok, status}
          :not_ready -> forward_response(backend, client)
        end

      {:error, reason} ->
        Logger.error("[Session] Backend recv error: #{inspect(reason)}")
        send_error(client, "backend read error")
        {:ok, :idle}
    end
  end

  # Forward packets until an extended-protocol unit terminator (not ReadyForQuery).
  defp forward_ext_unit(backend, client) do
    case :gen_tcp.recv(backend, 0, 5000) do
      {:ok, <<type, _::binary>> = packet} ->
        :ok = :gen_tcp.send(client, packet)

        cond do
          type == ?Z ->
            # ReadyForQuery arrived unexpectedly (error cascade) — stop
            :ok
          type in @ext_unit_terminators ->
            :ok
          true ->
            forward_ext_unit(backend, client)
        end

      {:error, reason} ->
        Logger.error("[Session] Backend recv (ext unit): #{inspect(reason)}")
    end
  end

  defp drain_response(socket) do
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, packet} ->
        case PgProtocol.ready_for_query_status(packet) do
          {:ok, _} -> :ok
          :not_ready -> drain_response(socket)
        end

      {:error, reason} ->
        Logger.error("[Session] Drain error: #{inspect(reason)}")
    end
  end

  # ── helpers ───────────────────────────────────────────────────────────

  defp active_ext_socket(%{tx_socket: s}) when s != nil, do: s
  defp active_ext_socket(%{ext_socket: s}), do: s

  defp begin?(sql), do: Regex.match?(~r/^\s*BEGIN\b/i, sql)

  defp cleanup(%{tx_socket: s, tx_shard: sh}) when s != nil, do: Pool.checkin(sh, s)
  defp cleanup(%{ext_socket: s, ext_shard: sh}) when s != nil, do: Pool.checkin(sh, s)
  defp cleanup(_), do: :ok

  # Fake responses for extended protocol buffer phase

  defp send_fake_parse_complete(socket) do
    :gen_tcp.send(socket, <<1, 4::32>>)
  end

  defp send_fake_ext_response(<<?B, _::binary>>, socket) do
    # BindComplete
    :gen_tcp.send(socket, <<2, 4::32>>)
  end

  defp send_fake_ext_response(<<?E, _::binary>>, socket) do
    # CommandComplete("BEGIN") for Execute of BEGIN
    send_command_complete(socket, "BEGIN")
  end

  defp send_fake_ext_response(<<?C, _::binary>>, socket) do
    # CloseComplete
    :gen_tcp.send(socket, <<3, 4::32>>)
  end

  defp send_fake_ext_response(<<?D, _::binary>>, socket) do
    # NoData for Describe
    :gen_tcp.send(socket, <<?n, 4::32>>)
  end

  defp send_fake_ext_response(_, _socket), do: :ok

  defp send_auth_ok(socket) do
    :gen_tcp.send(socket, [<<?R, 8::32, 0::32>>, <<?Z, 5::32, ?I>>])
  end

  defp send_command_complete(socket, tag) do
    body = tag <> <<0>>
    len = byte_size(body) + 4
    :gen_tcp.send(socket, <<?C, len::32, body::binary>>)
  end

  defp send_ready_for_query(socket, status) do
    byte = case status do
      :idle -> ?I
      :in_transaction -> ?T
      :failed -> ?E
    end
    :gen_tcp.send(socket, <<?Z, 5::32, byte>>)
  end

  defp send_error(socket, msg) do
    body = <<?S, "ERROR", 0, ?M, msg::binary, 0, 0>>
    len = byte_size(body) + 4
    :gen_tcp.send(socket, <<?E, len::32, body::binary>>)
    send_ready_for_query(socket, :idle)
  end
end
