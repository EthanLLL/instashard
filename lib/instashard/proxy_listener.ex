defmodule Instashard.ProxyListener do
  use GenServer
  require Logger

  # Matches a complete shard schema name. Change this when shard naming convention changes.
  # Full-string match (\A...\z) prevents partial matches like "old_shard_0001".
  @shard_pattern ~r/\Ashard_\d{4}\z/

  def start_link(opts) do
    port = Keyword.get(opts, :port, 5400)
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    # :binary
    # packet: 0
    # active: true - Async receive message，{:tcp, socket, data} -> handle_info
    # reuseaddr: true
    opts = [:binary, packet: 0, active: true, reuseaddr: true]

    case :gen_tcp.listen(port, opts) do
      {:ok, listen_socket} ->
        Logger.info(" [InstaShard] listening: #{port}")
        send(self(), :accept)
        state = %{listen_socket: listen_socket}
        {:ok, state}

      {:error, reason} ->
        Logger.error("Listening #{port} failed: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %{listen_socket: listen_socket} = state) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        Logger.info(" [InstaShard] New connection")

        # New a process to serve request
        {:ok, pid} =
          Task.start_link(fn ->
            serve_client(client_socket)
          end)

        # handle socket to task
        :ok = :gen_tcp.controlling_process(client_socket, pid)

        # Continue
        send(self(), :accept)
        {:noreply, state}

      {:error, reason} ->
        Logger.error("Failed to accept client connection: #{inspect(reason)}")
        send(self(), :accept)
        {:noreply, state}
    end
  end

  defp serve_client(client_socket) do
    # Init state
    state = %{
      client_socket: client_socket,
      # Lazy binding DB connection，nil
      backend_socket: nil,
      # Transaction status: :none, :pending_begin, :active
      tx_status: :none
    }

    # Event loop
    session_loop(state)
  end

  defp session_loop(state) do
    receive do
      # Packet from client
      {:tcp, src_socket, raw_packet} when src_socket == state.client_socket ->
        Logger.debug("Packet from client: #{inspect(raw_packet, limit: :infinity)}")

        # Handle packet
        new_state = handle_client_packet(raw_packet, state)
        session_loop(new_state)

      # Packet from server
      {:tcp, src_socket, raw_response} when src_socket == state.backend_socket ->
        # Send packet to client
        :ok = :gen_tcp.send(state.client_socket, raw_response)
        session_loop(state)

      # Connection closed by either client or server
      {:tcp_closed, _socket} ->
        Logger.info("Connection closed")
        if state.backend_socket, do: :gen_tcp.close(state.backend_socket)
        :gen_tcp.close(state.client_socket)
        :ok

      {:tcp_error, _socket, reason} ->
        Logger.error("TCP error: #{inspect(reason)}")
        if state.backend_socket, do: :gen_tcp.close(state.backend_socket)
        :gen_tcp.close(state.client_socket)
        :error
    end
  end

  # Match SSL
  defp handle_client_packet(<<0, 0, 0, 8, 4, 210, 22, 47>>, state) do
    Logger.info("SSL supported? No!")

    # Send 'N' (ASCII 78)
    :ok = :gen_tcp.send(state.client_socket, "N")

    state
  end

  # Match Login
  defp handle_client_packet(<<_len::32, 0, 3, 0, 0, payload::binary>>, state) do
  
    params = String.split(payload, <<0>>, trim: true)
  
    Logger.info("Params: #{inspect(params)}")

    config_map = 
      params
      |> Enum.chunk_every(2) 
      |> Map.new(fn [k, v] -> {k, v} end)

    target_db = Map.get(config_map, "database")
    user_name = Map.get(config_map, "user")

    Logger.info("#{user_name}\" connected to database: \"#{target_db}\"")

    :ok = :gen_tcp.send(state.client_socket, [<<"R", 8::32, 0::32>>, <<"Z", 5::32, "I">>])
  
    state
  end

  # Match SQL
  defp handle_client_packet(<<"Q", size::32, sql_with_null::binary>>, state) do
    sql_len = size - 4 - 1
  
    <<sql::binary-size(sql_len), _null::8>> = sql_with_null

    Logger.info("SQL: #{inspect(sql)}")

    case extract_shard(sql) do
      {:ok, shard_name} ->
        Logger.info("Shard: #{shard_name}")

        {:ok, backend_socket} = Instashard.Backend.Manager.get_socket(shard_name)

        original_packet = <<"Q", size::32, sql_with_null::binary>>
        :ok = :gen_tcp.send(backend_socket, original_packet)

        bridge_backend_to_client(backend_socket, state.client_socket)

        state

      :no_shard ->
        state
    end
  end

  # Strip string literals to avoid false matches inside quoted values,
  # then scan FROM/JOIN/INTO/UPDATE/TABLE clauses for schema.table references.
  # Returns {:ok, shard_name} for the first matching schema, or :no_shard.
  defp extract_shard(sql) do
    stripped = Regex.replace(~r/'(?:[^'\\]|\\.)*'/, sql, "''")

    # Captures the token immediately after the clause keyword, then checks
    # whether it is a qualified reference (schema.table) whose schema matches.
    schema_refs =
      Regex.scan(
        ~r/(?:FROM|JOIN|INTO|UPDATE|TABLE)\s+("?[A-Za-z_][A-Za-z0-9_]*"?)\."?[A-Za-z_][A-Za-z0-9_]*"?/i,
        stripped,
        capture: :all_but_first
      )
      |> List.flatten()
      |> Enum.map(&String.trim(&1, "\""))

    case Enum.find(schema_refs, &Regex.match?(@shard_pattern, &1)) do
      nil -> :no_shard
      shard_name -> {:ok, shard_name}
    end
  end

  defp bridge_backend_to_client(backend_socket, client_socket) do
    case :gen_tcp.recv(backend_socket, 0, 5000) do
      {:ok, packet} ->
        Logger.info("#{inspect(packet, limit: :infinity)}")
        :ok = :gen_tcp.send(client_socket, packet)
        
        prev_size = byte_size(packet) - 6

        case packet do
          <<_prev::binary-size(prev_size), "Z", 5::32, status::8>> ->
            case status do
              84 -> Logger.info("🏁 [物理库交卷] 状态: 'T' (In Transaction)。")
              73 -> Logger.info("🏁 [物理库交卷] 状态: 'I' (Idle)。")
              69 -> Logger.error("🚨 [物理库交卷] 状态: 'E' (Failed Transaction)！")
              unknown_status -> Logger.warning("What ?: #{unknown_status}")
            end
            :ok
          _ ->
            bridge_backend_to_client(backend_socket, client_socket)
        end
      {:error, reason} ->
        Logger.error("🚨 从物理库读取数据失败: #{inspect(reason)}")
    end 
  end

end
