defmodule Instashard.Backend.Manager do
  @moduledoc """
  Initialises the connection pool and shard→db mapping.
  Not on the hot path — Pool.checkout/checkin bypass this process entirely.

  Pool size is per-DB (stored in DbRegistry cfg.pool_size).
  fill_db/1 tops up connections for a db_id up to its configured pool_size.

  Public API for runtime use:
    replenish(db_id)             — top up pool for a db (called by Pool on socket retire)
    update_pool_size(db_id, n)   — change pool_size in Mnesia + persist + fill
  """

  use GenServer
  require Logger

  alias Instashard.Backend.{ConfigStore, Connection, DbRegistry, MigrationGate, Pool, ShardMapping, StmtCache}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Top up connections for a db_id. Called by Pool when a socket is retired."
  def replenish(db_id), do: GenServer.call(__MODULE__, {:replenish, db_id})

  @doc "Update pool_size for a db, persist to file, and immediately fill to new target."
  def update_pool_size(db_id, new_size) when is_integer(new_size) and new_size >= 0 do
    GenServer.call(__MODULE__, {:update_pool_size, db_id, new_size})
  end

  @impl true
  def init(_opts) do
    Pool.init()
    StmtCache.init()
    ShardMapping.init()
    DbRegistry.init()
    MigrationGate.init()

    seed_from_files()

    send(self(), :fill_pool)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:fill_pool, state) do
    DbRegistry.all()
    |> Enum.each(fn {db_id, _cfg} -> fill_db(db_id) end)
    {:noreply, state}
  end

  @impl true
  def handle_call({:replenish, db_id}, _from, state) do
    fill_db(db_id)
    {:reply, :ok, state}
  end

  def handle_call({:update_pool_size, db_id, new_size}, _from, state) do
    result = with {:ok, cfg} <- DbRegistry.get(db_id) do
      :ok = DbRegistry.put(db_id, %{cfg | pool_size: new_size})
      case ConfigStore.persist_databases() do
        :ok -> :ok
        {:error, r} -> Logger.error("[Manager] Failed to persist databases.json: #{inspect(r)}")
      end
      fill_db(db_id)
      Logger.info("[Manager] pool_size for #{db_id} updated to #{new_size}")
      :ok
    end
    {:reply, result, state}
  end

  # ── Seed ──────────────────────────────────────────────────────────────

  defp seed_from_files do
    case ConfigStore.load_databases() do
      {:ok, dbs} ->
        Enum.each(dbs, fn %{id: id} = db ->
          DbRegistry.put_new(id, Map.drop(db, [:id]))
        end)
        Logger.info("[Manager] Seeded #{length(dbs)} database(s) from databases.json")
      {:error, reason} ->
        Logger.error("[Manager] Failed to load databases.json: #{inspect(reason)}")
    end

    case ConfigStore.load_shards() do
      {:ok, pairs} ->
        Enum.each(pairs, fn {shard, db_id} -> ShardMapping.put(shard, db_id) end)
        Logger.info("[Manager] Seeded #{length(pairs)} shard mapping(s) from shards.json")
      {:error, reason} ->
        Logger.error("[Manager] Failed to load shards.json: #{inspect(reason)}")
    end
  end

  # ── Pool fill ─────────────────────────────────────────────────────────

  defp fill_db(db_id) do
    case DbRegistry.get(db_id) do
      {:ok, cfg} ->
        current = Pool.count(db_id)
        delta   = cfg.pool_size - current
        cond do
          delta > 0 ->
            Enum.each(1..delta, fn _ ->
              case Connection.connect(cfg) do
                {:ok, socket} ->
                  Pool.put(db_id, Pool.new_entry(socket))
                  Logger.debug("[Manager] Connected socket for #{db_id}")
                {:error, reason} ->
                  Logger.error("[Manager] Failed to connect for #{db_id}: #{inspect(reason)}")
              end
            end)
          delta < 0 ->
            Pool.trim(db_id, -delta)
          true -> :ok
        end
      {:error, :not_found} ->
        Logger.error("[Manager] No db config for #{db_id}")
    end
  end
end
