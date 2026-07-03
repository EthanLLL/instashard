defmodule Instashard.Backend.Migration do
  @moduledoc """
  Migration state machine GenServer. Coordinates live shard migration between physical DBs.

  States:
    :idle          → start_migration/2 →
    :preparing     schema clone + publication/subscription setup
    :replicating   polling replication lag every 5s
    :draining      gate → :closing, waiting for pool to drain
    :cutting_over  update ShardMapping, gate → :open, notify waiters
    :idle          cleanup replication objects

  Public API:
    start_migration(shard, target_db_key)
    status(shard)
    drain(shard)
    cutover(shard)
    cancel(shard)
  """

  use GenServer
  require Logger

  alias Instashard.Backend.{Connection, MigrationGate, Pool, SchemaCloner, ShardMapping}

  @lag_poll_ms 5_000
  @pub_suffix "_instashard_pub"
  @sub_suffix "_instashard_sub"

  defstruct [
    phase: :idle,
    shard: nil,
    source_cfg: nil,
    target_cfg: nil,
    source_socket: nil,  # dedicated connection for replication monitoring
    target_socket: nil,  # dedicated connection for subscription management
    lag_bytes: nil,
    error: nil
  ]

  # ── Public API ────────────────────────────────────────────────────────

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def start_migration(shard, target_db_key) do
    GenServer.call(__MODULE__, {:start_migration, shard, target_db_key})
  end

  def status(shard) do
    GenServer.call(__MODULE__, {:status, shard})
  end

  def drain(shard) do
    GenServer.call(__MODULE__, {:drain, shard})
  end

  def cutover(shard) do
    GenServer.call(__MODULE__, {:cutover, shard})
  end

  def cancel(shard) do
    GenServer.call(__MODULE__, {:cancel, shard})
  end

  # ── GenServer ─────────────────────────────────────────────────────────

  @impl true
  def init(_opts), do: {:ok, %__MODULE__{}}

  @impl true
  def handle_call({:start_migration, shard, target_db_key}, _from, %{phase: :idle} = state) do
    with {:ok, source_cfg} <- ShardMapping.lookup(shard),
         {:ok, target_cfg} <- lookup_db_config(target_db_key) do
      new_state = %{state | phase: :preparing, shard: shard,
                             source_cfg: source_cfg, target_cfg: target_cfg}
      send(self(), :prepare)
      {:reply, :ok, new_state}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call({:start_migration, _shard, _target}, _from, state) do
    {:reply, {:error, {:busy, state.phase}}, state}
  end

  def handle_call({:status, shard}, _from, state) do
    if state.shard == shard do
      {:reply, {:ok, %{phase: state.phase, lag_bytes: state.lag_bytes, error: state.error}}, state}
    else
      {:reply, {:ok, %{phase: :idle}}, state}
    end
  end

  def handle_call({:drain, shard}, _from, %{phase: :replicating, shard: shard} = state) do
    MigrationGate.set_status(shard, :closing)
    send(self(), :await_drain)
    {:reply, :ok, %{state | phase: :draining}}
  end

  def handle_call({:drain, _shard}, _from, state) do
    {:reply, {:error, {:wrong_phase, state.phase}}, state}
  end

  def handle_call({:cutover, shard}, _from, %{phase: :draining, shard: shard} = state) do
    case state.lag_bytes do
      0 ->
        send(self(), :do_cutover)
        {:reply, :ok, %{state | phase: :cutting_over}}
      lag ->
        {:reply, {:error, {:lag_not_zero, lag}}, state}
    end
  end

  def handle_call({:cutover, _shard}, _from, state) do
    {:reply, {:error, {:wrong_phase, state.phase}}, state}
  end

  def handle_call({:cancel, shard}, _from, state) when state.shard == shard do
    new_state = do_cancel(state)
    {:reply, :ok, new_state}
  end

  def handle_call({:cancel, _}, _from, state) do
    {:reply, {:error, :not_migrating}, state}
  end

  # ── Internal messages ─────────────────────────────────────────────────

  @impl true
  def handle_info(:prepare, state) do
    Logger.info("[Migration] Preparing migration for #{state.shard}")

    with {:ok, src} <- Connection.connect(state.source_cfg),
         {:ok, tgt} <- Connection.connect(state.target_cfg),
         :ok <- SchemaCloner.clone(state.shard, src, tgt),
         :ok <- setup_publication(src, state.shard),
         :ok <- setup_subscription(tgt, state.shard, state.source_cfg) do

      Logger.info("[Migration] #{state.shard} replication started")
      Process.send_after(self(), :poll_lag, @lag_poll_ms)
      {:noreply, %{state | phase: :replicating, source_socket: src, target_socket: tgt}}
    else
      {:error, reason} ->
        Logger.error("[Migration] Prepare failed: #{inspect(reason)}")
        {:noreply, %{state | phase: :idle, error: reason}}
    end
  end

  def handle_info(:poll_lag, %{phase: phase} = state) when phase in [:replicating, :draining] do
    lag = query_lag(state.source_socket, state.shard)
    Logger.debug("[Migration] #{state.shard} lag=#{inspect(lag)} bytes")
    Process.send_after(self(), :poll_lag, @lag_poll_ms)
    {:noreply, %{state | lag_bytes: lag}}
  end

  def handle_info(:poll_lag, state), do: {:noreply, state}

  def handle_info(:await_drain, state) do
    nodes = [node() | Node.list()]
    target_size = Instashard.Backend.Manager.pool_size() * length(nodes)
    results = :erpc.multicall(nodes, Pool, :count, [state.shard])
    pool_size = results |> Enum.filter(&match?({:ok, _}, &1)) |> Enum.map(&elem(&1, 1)) |> Enum.sum()

    if pool_size >= target_size do
      Logger.info("[Migration] #{state.shard} pool drained across #{length(nodes)} node(s), ready for cutover (lag=#{state.lag_bytes} bytes)")
      MigrationGate.set_status(state.shard, :closed)
    else
      Logger.debug("[Migration] #{state.shard} waiting for drain: #{pool_size}/#{target_size} idle across #{length(nodes)} node(s)")
      Process.send_after(self(), :await_drain, 500)
    end
    {:noreply, state}
  end

  def handle_info(:do_cutover, state) do
    Logger.info("[Migration] Cutting over #{state.shard} to new db")

    # 1. Update shard→db mapping
    :ok = ShardMapping.put(state.shard, state.target_cfg)

    # 2. Flush old idle connections on all nodes (they point to the old db)
    nodes = [node() | Node.list()]
    :erpc.multicall(nodes, Pool, :flush, [state.shard])

    # 3. Reopen gate (points to new db now)
    MigrationGate.set_status(state.shard, :open)

    # 4. Replenish pool on all nodes from new db
    :erpc.multicall(nodes, Instashard.Backend.Manager, :replenish, [state.shard])

    # 5. Resume waiting sessions
    MigrationGate.notify_waiters(state.shard)

    Logger.info("[Migration] #{state.shard} cutover complete")

    # 6. Cleanup replication objects (best-effort)
    cleanup_replication(state)

    {:noreply, reset_state(state)}
  end

  # ── Replication helpers ───────────────────────────────────────────────

  defp setup_publication(socket, shard) do
    pub = pub_name(shard)
    # Collect all tables in the shard schema
    {:ok, tables} = SchemaCloner.simple_query(
      socket,
      "SELECT tablename FROM pg_tables WHERE schemaname = '#{shard}' ORDER BY tablename"
    )
    table_list = tables
      |> Enum.map(fn [t] -> ~s("#{shard}"."#{t}") end)
      |> Enum.join(", ")

    drop = "DROP PUBLICATION IF EXISTS #{pub}"
    create = "CREATE PUBLICATION #{pub} FOR TABLE #{table_list}"

    with {:ok, _} <- SchemaCloner.simple_query(socket, drop),
         {:ok, _} <- SchemaCloner.simple_query(socket, create) do
      Logger.info("[Migration] Publication #{pub} created")
      :ok
    end
  end

  defp setup_subscription(target_socket, shard, source_cfg) do
    sub = sub_name(shard)
    pub = pub_name(shard)
    connstr = db_connstr(source_cfg)

    # Disable + detach slot before drop so the slot on source is not auto-dropped with it.
    # This leaves the slot as an orphan — we clean it up explicitly on source below.
    SchemaCloner.simple_query(target_socket, "ALTER SUBSCRIPTION #{sub} DISABLE")
    SchemaCloner.simple_query(target_socket, "ALTER SUBSCRIPTION #{sub} SET (slot_name = NONE)")
    SchemaCloner.simple_query(target_socket, "DROP SUBSCRIPTION IF EXISTS #{sub}")

    # Drop orphan slot on source if it exists (from a previous failed attempt).
    with {:ok, src} <- Connection.connect(source_cfg) do
      SchemaCloner.simple_query(src, "SELECT pg_drop_replication_slot('#{sub}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '#{sub}')")
      :gen_tcp.close(src)
    end

    create = """
    CREATE SUBSCRIPTION #{sub}
      CONNECTION '#{connstr}'
      PUBLICATION #{pub}
    """

    case SchemaCloner.simple_query(target_socket, create) do
      {:ok, _} ->
        Logger.info("[Migration] Subscription #{sub} created")
        :ok
      err -> err
    end
  end

  defp query_lag(source_socket, shard) do
    slot = sub_name(shard)
    sql = """
    SELECT
      CASE
        WHEN confirmed_flush_lsn IS NULL THEN NULL
        ELSE pg_current_wal_lsn() - confirmed_flush_lsn
      END
    FROM pg_replication_slots
    WHERE slot_name = '#{slot}'
    """
    case SchemaCloner.simple_query(source_socket, sql) do
      {:ok, [[nil]]} -> nil
      {:ok, [[lag]]} -> String.to_integer(lag)
      {:ok, []} -> nil
      _ -> nil
    end
  end

  defp cleanup_replication(%{source_socket: src, target_socket: tgt, shard: shard}) do
    sub = sub_name(shard)
    pub = pub_name(shard)
    # Detach slot before drop so we can delete it explicitly on source
    SchemaCloner.simple_query(tgt, "ALTER SUBSCRIPTION #{sub} DISABLE")
    SchemaCloner.simple_query(tgt, "ALTER SUBSCRIPTION #{sub} SET (slot_name = NONE)")
    SchemaCloner.simple_query(tgt, "DROP SUBSCRIPTION IF EXISTS #{sub}")
    # Drop the now-orphaned slot on source, then publication
    SchemaCloner.simple_query(src, "SELECT pg_drop_replication_slot('#{sub}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '#{sub}')")
    SchemaCloner.simple_query(src, "DROP PUBLICATION IF EXISTS #{pub}")
    :ok
  end

  defp do_cancel(state) do
    if state.phase != :idle do
      MigrationGate.set_status(state.shard, :open)
      MigrationGate.notify_waiters(state.shard)
      cleanup_replication(state)
      Logger.info("[Migration] #{state.shard} migration cancelled")
    end
    reset_state(state)
  end

  defp reset_state(state) do
    if state.source_socket, do: :gen_tcp.close(state.source_socket)
    if state.target_socket, do: :gen_tcp.close(state.target_socket)
    %__MODULE__{}
  end

  # ── Config helpers ────────────────────────────────────────────────────

  # Hardcoded for now — same as Manager seed. Will move to a shared config module.
  @db_configs %{
    db0: %{host: "127.0.0.1", port: 5430, username: "postgres", password: "luozhenzuishuai", database: "my_cluster"},
    db1: %{host: "127.0.0.1", port: 5431, username: "postgres", password: "luozhenzuishuai", database: "my_cluster"}
  }

  defp lookup_db_config(db_key) do
    case Map.fetch(@db_configs, db_key) do
      {:ok, cfg} -> {:ok, cfg}
      :error -> {:error, {:unknown_db, db_key}}
    end
  end

  defp db_connstr(%{host: h, port: p, username: u, password: pw, database: db}) do
    "host=#{h} port=#{p} dbname=#{db} user=#{u} password=#{pw}"
  end

  defp pub_name(shard), do: "#{shard}#{@pub_suffix}"
  defp sub_name(shard), do: "#{shard}#{@sub_suffix}"
end
