defmodule Instashard.Backend.Manager do
  @moduledoc """
  Manages connection pool lifecycle. Not on the hot path.

  state.pool_sizes = %{db_id => target_count}

  Pool size is the authoritative target connection count per db, maintained in
  Manager state. DbRegistry still stores pool_size for persistence but Manager
  state is the live source of truth.

  Public API:
    discard(db_id, socket)        — close dead socket, add one replacement
    update_pool_size(db_id, n)    — adjust target, add/trim connections, persist
  """

  use GenServer
  require Logger

  alias Instashard.Backend.{ConfigStore, Connection, DbRegistry, MigrationGate, Pool, ShardMapping, StmtCache}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Register a newly added db and fill its initial pool."
  def add_db(db_id), do: GenServer.cast(__MODULE__, {:add_db, db_id})

  @doc "Discard a dead socket and add one replacement connection."
  def discard(db_id, socket), do: GenServer.cast(__MODULE__, {:discard, db_id, socket})

  @doc "Update target pool size for a db, persist to file, add/trim connections."
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

    pool_sizes = seed_from_files()

    send(self(), :fill_pool)
    {:ok, %{pool_sizes: pool_sizes}}
  end

  @impl true
  def handle_info(:fill_pool, state) do
    Enum.each(state.pool_sizes, fn {db_id, n} ->
      Enum.each(1..n, fn _ -> add_connection(db_id) end)
    end)
    {:noreply, state}
  end

  @impl true
  def handle_cast({:add_db, db_id}, state) do
    case DbRegistry.get(db_id) do
      {:ok, cfg} ->
        n = cfg.pool_size
        Enum.each(1..n, fn _ -> add_connection(db_id) end)
        {:noreply, %{state | pool_sizes: Map.put(state.pool_sizes, db_id, n)}}
      {:error, :not_found} ->
        Logger.error("[Manager] add_db: no config for #{db_id}")
        {:noreply, state}
    end
  end

  def handle_cast({:discard, db_id, socket}, state) do
    :gen_tcp.close(socket)
    add_connection(db_id)
    {:noreply, state}
  end

  @impl true
  def handle_call({:update_pool_size, db_id, new_size}, _from, state) do
    old_size = Map.get(state.pool_sizes, db_id, 0)
    delta = new_size - old_size

    result = with {:ok, cfg} <- DbRegistry.get(db_id) do
      :ok = DbRegistry.put(db_id, %{cfg | pool_size: new_size})
      case ConfigStore.persist_databases() do
        :ok -> :ok
        {:error, r} -> Logger.error("[Manager] Failed to persist databases.json: #{inspect(r)}")
      end
      cond do
        delta > 0 -> Enum.each(1..delta, fn _ -> add_connection(db_id) end)
        delta < 0 -> Pool.trim(db_id, -delta)
        true -> :ok
      end
      Logger.info("[Manager] pool_size for #{db_id}: #{old_size} → #{new_size}")
      :ok
    end

    new_sizes = Map.put(state.pool_sizes, db_id, new_size)
    {:reply, result, %{state | pool_sizes: new_sizes}}
  end

  # ── Seed ──────────────────────────────────────────────────────────────

  # Returns %{db_id => pool_size} for Manager state
  defp seed_from_files do
    pool_sizes =
      case ConfigStore.load_databases() do
        {:ok, dbs} ->
          Enum.each(dbs, fn %{id: id} = db ->
            DbRegistry.put_new(id, Map.drop(db, [:id]))
          end)
          Logger.info("[Manager] Seeded #{length(dbs)} database(s) from databases.json")
          Map.new(dbs, fn %{id: id, pool_size: n} -> {id, n} end)
        {:error, reason} ->
          Logger.error("[Manager] Failed to load databases.json: #{inspect(reason)}")
          %{}
      end

    case ConfigStore.load_shards() do
      {:ok, pairs} ->
        Enum.each(pairs, fn {shard, db_id} -> ShardMapping.put(shard, db_id) end)
        Logger.info("[Manager] Seeded #{length(pairs)} shard mapping(s) from shards.json")
      {:error, reason} ->
        Logger.error("[Manager] Failed to load shards.json: #{inspect(reason)}")
    end

    pool_sizes
  end

  # ── Connection management ─────────────────────────────────────────────

  defp add_connection(db_id) do
    case DbRegistry.get(db_id) do
      {:ok, cfg} ->
        case Connection.connect(cfg) do
          {:ok, socket} ->
            Pool.put(db_id, Pool.new_entry(socket))
            Logger.debug("[Manager] New connection for #{db_id}")
          {:error, reason} ->
            Logger.error("[Manager] Failed to connect for #{db_id}: #{inspect(reason)}")
        end
      {:error, :not_found} ->
        Logger.error("[Manager] No db config for #{db_id}")
    end
  end
end
