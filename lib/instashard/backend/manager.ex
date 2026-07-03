defmodule Instashard.Backend.Manager do
  @moduledoc """
  Initialises the connection pool and shard→db mapping.
  Not on the hot path — Pool.checkout/checkin bypass this process entirely.
  Shard mapping lives in Mnesia (ShardMapping); Manager seeds it at startup.
  """

  use GenServer
  require Logger

  @pool_size 5

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Synchronously replenish connections for a shard. Safe to call before notifying waiters."
  def replenish(shard), do: GenServer.call(__MODULE__, {:replenish, shard})

  @doc "Current target pool size per shard."
  def pool_size, do: @pool_size

  @impl true
  def init(_opts) do
    Instashard.Backend.Pool.init()
    Instashard.Backend.StmtCache.init()
    Instashard.Backend.ShardMapping.init()
    Instashard.Backend.MigrationGate.init()

    seed_mapping()

    send(self(), :fill_pool)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:fill_pool, state) do
    Instashard.Backend.ShardMapping.all()
    |> Enum.each(fn {shard, _cfg} -> fill_shard(shard) end)
    {:noreply, state}
  end

  @impl true
  def handle_call({:replenish, shard}, _from, state) do
    fill_shard(shard)
    {:reply, :ok, state}
  end

  defp seed_mapping do
    db_configs = %{
      db0: %{host: "127.0.0.1", port: 5430, username: "postgres", password: "luozhenzuishuai", database: "my_cluster"},
      db1: %{host: "127.0.0.1", port: 5431, username: "postgres", password: "luozhenzuishuai", database: "my_cluster"}
    }

    shard_assignments = %{
      "shard_0000" => :db0,
      "shard_0001" => :db1,
      "shard_0002" => :db0
    }

    Enum.each(shard_assignments, fn {shard, db_key} ->
      cfg = Map.fetch!(db_configs, db_key)
      Instashard.Backend.ShardMapping.put(shard, cfg)
    end)
  end

  defp fill_shard(shard) do
    current = Instashard.Backend.Pool.count(shard)
    needed = @pool_size - current

    if needed > 0 do
      case Instashard.Backend.ShardMapping.lookup(shard) do
        {:ok, cfg} ->
          Enum.each(1..needed, fn _ ->
            case Instashard.Backend.Connection.connect(cfg) do
              {:ok, socket} ->
                entry = Instashard.Backend.Pool.new_entry(socket)
                Instashard.Backend.Pool.checkin(shard, entry)
                Logger.debug("[Manager] Connected socket for #{shard}")
              {:error, reason} ->
                Logger.error("[Manager] Failed to connect for #{shard}: #{inspect(reason)}")
            end
          end)

        {:error, :not_found} ->
          Logger.error("[Manager] No db config for #{shard}")
      end
    end
  end
end
