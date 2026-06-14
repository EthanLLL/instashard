defmodule Instashard.Backend.Manager do
  use GenServer
  require Logger

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_socket(shard) do
    GenServer.call(__MODULE__, {:get_socket, shard})
  end

  @impl true
  def init(_opts) do
    Logger.info("Init Backend Manager")

    shard_mapping = %{
      "shard_0000" => :db0,
      "shard_0001" => :db1
    }

    db_config = %{
      db0: %{
        name: "db0",
        host: ~c"127.0.0.1",
        port: 5430,
        username: "postgres",
        password: "luozhenzuishuai",
        database: "my_cluster"
      },
      db1: %{
        name: "db1",
        host: ~c"127.0.0.1",
        port: 5431,
        username: "postgres",
        password: "luozhenzuishuai",
        database: "my_cluster"
      }
    }

    sockets = %{
      db0: init_connection(db_config.db0),
      db1: init_connection(db_config.db1)
    }

    {:ok, %{shard_mapping: shard_mapping, db_config: db_config, sockets: sockets}}
  end

  defp init_connection(db) do
    {:ok, socket} = :gen_tcp.connect(db.host, db.port, [:binary, active: false])

    payload = <<
      # Protocol version (3.0)
      0,
      3,
      0,
      0,
      "user",
      0,
      db.username::binary,
      0,
      "database",
      0,
      db.database::binary,
      0,
      0
    >>

    packet_size = byte_size(payload) + 4

    startup_packet = <<packet_size::32, payload::binary>>

    :ok = :gen_tcp.send(socket, startup_packet)

    case :gen_tcp.recv(socket, 0, 2000) do
      {:ok, _trash} ->
        Logger.info("#{db.name} connected")
        socket

      {error, :timeout} ->
        Logger.error("#{error}")
    end
  end

  # defp contains_ready_for_query?(<<_::binary, "Z", 5::32, status::8, _::binary>>)
  #      when status in [73, 84, 69] do
  #   # 状态码：73='I', 84='T', 69='E'
  #   # 只要命中了这个结构的切片，直接返回 true
  #   true
  # end
  #
  # defp contains_ready_for_query?(_), do: false

  @impl true
  def handle_call({:get_socket, shard}, _from, state) do
    case Map.get(state.shard_mapping, shard) do
      nil ->
        Logger.error("No shard mapping: #{shard}")
        {:reply, {:error, :unmapped_shard}, state}

      db_key ->
        socket = Map.get(state.sockets, db_key)
        {:reply, {:ok, socket}, state}
    end
  end
end
