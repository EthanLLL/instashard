defmodule Instashard.Backend.Manager do
  @moduledoc """
  Manages shard→physical-db mapping and connection replenishment.
  Not on the hot path — Pool.checkout/checkin bypass this process entirely.
  """

  use GenServer
  require Logger

  # How many idle connections to maintain per shard.
  @pool_size 5

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Notify the manager to replenish connections for a shard."
  def replenish(shard), do: send(__MODULE__, {:replenish, shard})

  @impl true
  def init(_opts) do
    Instashard.Backend.Pool.init()

    shard_map = %{
      "shard_0000" => :db0,
      "shard_0001" => :db1
    }

    db_config = %{
      db0: %{host: "127.0.0.1", port: 5430, username: "postgres", password: "luozhenzuishuai", database: "my_cluster"},
      db1: %{host: "127.0.0.1", port: 5431, username: "postgres", password: "luozhenzuishuai", database: "my_cluster"}
    }

    state = %{shard_map: shard_map, db_config: db_config}
    send(self(), :fill_pool)
    {:ok, state}
  end

  @impl true
  def handle_info(:fill_pool, state) do
    Enum.each(state.shard_map, fn {shard, _db_key} ->
      fill_shard(shard, state)
    end)
    {:noreply, state}
  end

  def handle_info({:replenish, shard}, state) do
    fill_shard(shard, state)
    {:noreply, state}
  end

  defp fill_shard(shard, state) do
    current = Instashard.Backend.Pool.count(shard)
    needed = @pool_size - current

    if needed > 0 do
      db_key = Map.fetch!(state.shard_map, shard)
      cfg = Map.fetch!(state.db_config, db_key)

      Enum.each(1..needed, fn _ ->
        case Instashard.Backend.Connection.connect(cfg) do
          {:ok, socket} ->
            Instashard.Backend.Pool.checkin(shard, socket)
            Logger.debug("[Manager] Connected socket for #{shard}")

          {:error, reason} ->
            Logger.error("[Manager] Failed to connect for #{shard}: #{inspect(reason)}")
        end
      end)
    end
  end
end
