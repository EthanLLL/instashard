defmodule Instashard.Migration.Worker do
  @moduledoc """
  Per-shard migration worker. Registered in Horde.Registry under the shard name.
  On restart, recovers phase from ShardRoute status and reconnects sockets.

  Phases:
    :preparing    schema clone + publication/subscription setup
    :replicating  polling replication lag every 5s
    :draining     gate → :closing, waiting for active_tx → 0
    :cutting_over update ShardRoute db_id, gate → :open, notify waiters
  """

  use GenServer, restart: :transient
  require Logger

  alias Instashard.Backend.{ConfigStore, Connection, DbRegistry, Pool, SchemaCloner, ShardRoute}
  alias InstashardWeb.AdminChannel

  @lag_poll_ms 5_000
  @pub_suffix "_instashard_pub"
  @sub_suffix "_instashard_sub"

  defstruct [
    shard: nil,
    source_db_id: nil,
    target_db_id: nil,
    source_cfg: nil,
    target_cfg: nil,
    source_socket: nil,
    target_socket: nil,
    lag_bytes: nil,
    phase: :preparing,
    error: nil
  ]

  # ── Child spec ────────────────────────────────────────────────────────

  def child_spec(opts) do
    shard = Keyword.fetch!(opts, :shard)
    %{
      id: {__MODULE__, shard},
      start: {__MODULE__, :start_link, [opts]},
      restart: :transient
    }
  end

  def start_link(opts) do
    shard = Keyword.fetch!(opts, :shard)
    GenServer.start_link(__MODULE__, opts,
      name: {:via, Horde.Registry, {Instashard.Migration.Registry, shard}}
    )
  end

  # ── Public API ────────────────────────────────────────────────────────

  def drain(shard),   do: with_worker(shard, &GenServer.call(&1, :drain))
  def cutover(shard), do: with_worker(shard, &GenServer.call(&1, :cutover))
  def cancel(shard),  do: with_worker(shard, &GenServer.call(&1, :cancel))
  def status(shard),  do: with_worker(shard, &GenServer.call(&1, :status))

  defp with_worker(shard, fun) do
    case Instashard.Migration.Supervisor.worker_pid(shard) do
      {:ok, pid} -> fun.(pid)
      {:error, :not_found} -> {:error, :not_migrating}
    end
  end

  # ── Init ──────────────────────────────────────────────────────────────

  @impl true
  def init(opts) do
    shard = Keyword.fetch!(opts, :shard)
    target_db_id = Keyword.fetch!(opts, :target_db_id)

    with {:ok, source_db_id} <- ShardRoute.lookup(shard),
         :ok <- if(source_db_id == target_db_id, do: {:error, :same_db}, else: :ok),
         {:ok, source_cfg} <- DbRegistry.get(source_db_id),
         {:ok, target_cfg} <- DbRegistry.get(target_db_id) do

      state = %__MODULE__{
        shard: shard,
        source_db_id: source_db_id,
        target_db_id: target_db_id,
        source_cfg: source_cfg,
        target_cfg: target_cfg
      }

      # Recover phase from gate status (handles worker restart mid-migration)
      gate = ShardRoute.status(shard)
      state = recover_phase(state, gate)
      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  # Recover phase after a worker restart based on gate status
  defp recover_phase(state, :open) do
    # Gate is open → either fresh start or restarted during replicating
    # Check if PG subscription already exists → replicating
    case Connection.connect(state.target_cfg) do
      {:ok, tgt} ->
        case check_subscription_exists(tgt, state.shard) do
          true ->
            Logger.info("[Migration] #{state.shard} recovering → replicating")
            AdminChannel.broadcast_migration_event(state.shard, "replicating", "recovered after restart")
            {:ok, src} = Connection.connect(state.source_cfg)
            Process.send_after(self(), :poll_lag, 500)
            %{state | phase: :replicating, source_socket: src, target_socket: tgt}
          false ->
            # Fresh start
            send(self(), :prepare)
            AdminChannel.broadcast_migration_event(state.shard, "preparing",
              "source=#{state.source_db_id} target=#{state.target_db_id}")
            %{state | phase: :preparing, target_socket: tgt}
        end
      {:error, _} ->
        send(self(), :prepare)
        %{state | phase: :preparing}
    end
  end

  defp recover_phase(state, :closing) do
    Logger.info("[Migration] #{state.shard} recovering → draining")
    AdminChannel.broadcast_migration_event(state.shard, "draining", "recovered after restart")
    {:ok, src} = Connection.connect(state.source_cfg)
    {:ok, tgt} = Connection.connect(state.target_cfg)
    Process.send_after(self(), :poll_lag, 500)
    send(self(), :await_drain)
    %{state | phase: :draining, source_socket: src, target_socket: tgt}
  end

  defp recover_phase(state, :closed) do
    Logger.info("[Migration] #{state.shard} recovering → drained, awaiting cutover")
    AdminChannel.broadcast_migration_event(state.shard, "drained", "recovered after restart, awaiting cutover")
    {:ok, src} = Connection.connect(state.source_cfg)
    {:ok, tgt} = Connection.connect(state.target_cfg)
    Process.send_after(self(), :poll_lag, 500)
    %{state | phase: :draining, source_socket: src, target_socket: tgt}
  end

  # ── handle_call ───────────────────────────────────────────────────────

  @impl true
  def handle_call(:drain, _from, %{phase: :replicating} = state) do
    ShardRoute.set_status(state.shard, :closing)
    send(self(), :await_drain)
    AdminChannel.broadcast_migration_event(state.shard, "draining", "gate closing")
    {:reply, :ok, %{state | phase: :draining}}
  end

  def handle_call(:drain, _from, state) do
    {:reply, {:error, {:wrong_phase, state.phase}}, state}
  end

  def handle_call(:cutover, _from, %{phase: :draining} = state) do
    case state.lag_bytes do
      0 ->
        send(self(), :do_cutover)
        {:reply, :ok, %{state | phase: :cutting_over}}
      lag ->
        {:reply, {:error, {:lag_not_zero, lag}}, state}
    end
  end

  def handle_call(:cutover, _from, state) do
    {:reply, {:error, {:wrong_phase, state.phase}}, state}
  end

  def handle_call(:cancel, _from, state) do
    new_state = do_cancel(state)
    {:reply, :ok, new_state}
  end

  def handle_call(:status, _from, state) do
    status = %{
      shard: state.shard,
      source_db_id: state.source_db_id,
      target_db_id: state.target_db_id,
      phase: state.phase,
      lag_bytes: state.lag_bytes,
      gate: ShardRoute.status(state.shard)
    }
    {:reply, status, state}
  end

  # ── handle_info ───────────────────────────────────────────────────────

  @impl true
  def handle_info(:prepare, state) do
    Logger.info("[Migration] Preparing #{state.shard}")
    src = state.source_socket
    tgt = state.target_socket

    with {:ok, src} <- (if src, do: {:ok, src}, else: Connection.connect(state.source_cfg)),
         {:ok, tgt} <- (if tgt, do: {:ok, tgt}, else: Connection.connect(state.target_cfg)),
         :ok <- SchemaCloner.clone(state.shard, src, tgt),
         :ok <- setup_publication(src, state.shard),
         :ok <- setup_subscription(tgt, state.shard, state.source_cfg) do

      AdminChannel.broadcast_migration_event(state.shard, "replicating", "replication started")
      Process.send_after(self(), :poll_lag, @lag_poll_ms)
      {:noreply, %{state | phase: :replicating, source_socket: src, target_socket: tgt}}
    else
      {:error, reason} ->
        Logger.error("[Migration] Prepare failed: #{inspect(reason)}")
        AdminChannel.broadcast_migration_event(state.shard, "error", inspect(reason))
        {:stop, {:shutdown, reason}, %{state | error: reason}}
    end
  end

  def handle_info(:poll_lag, %{phase: phase} = state) when phase in [:replicating, :draining] do
    lag = query_lag(state.source_socket, state.shard)
    Logger.debug("[Migration] #{state.shard} lag=#{inspect(lag)} bytes")
    AdminChannel.broadcast_migration_event(state.shard, "lag", "#{inspect(lag)} bytes")
    Process.send_after(self(), :poll_lag, @lag_poll_ms)
    {:noreply, %{state | lag_bytes: lag}}
  end

  def handle_info(:poll_lag, state), do: {:noreply, state}

  def handle_info(:await_drain, state) do
    nodes = [node() | Node.list()]
    results = :erpc.multicall(nodes, Pool, :active_tx_count, [state.shard])
    active = results |> Enum.filter(&match?({:ok, _}, &1)) |> Enum.map(&elem(&1, 1)) |> Enum.sum()

    if active == 0 do
      ShardRoute.set_status(state.shard, :closed)
      AdminChannel.broadcast_migration_event(state.shard, "drained",
        "ready for cutover, lag=#{state.lag_bytes} bytes")
    else
      AdminChannel.broadcast_migration_event(state.shard, "draining", "#{active} active tx")
      Process.send_after(self(), :await_drain, 500)
    end
    {:noreply, state}
  end

  def handle_info(:do_cutover, state) do
    Logger.info("[Migration] Cutting over #{state.shard} → #{state.target_db_id}")

    :ok = ShardRoute.put(state.shard, state.target_db_id)
    case ConfigStore.persist_shards() do
      :ok -> :ok
      {:error, r} -> Logger.error("[Migration] persist_shards failed: #{inspect(r)}")
    end

    ShardRoute.set_status(state.shard, :open)
    ShardRoute.notify_waiters(state.shard)

    AdminChannel.broadcast_migration_event(state.shard, "cutover_complete",
      "now on #{state.target_db_id}")
    Logger.info("[Migration] #{state.shard} cutover complete")

    cleanup_replication(state)
    drop_source_schema(state)

    Horde.DynamicSupervisor.terminate_child(Instashard.Migration.Supervisor, self())
    {:noreply, state}
  end

  # ── Replication helpers ───────────────────────────────────────────────

  defp check_subscription_exists(socket, shard) do
    sub = sub_name(shard)
    sql = "SELECT 1 FROM pg_subscription WHERE subname = '#{escape(sub)}'"
    case SchemaCloner.simple_query(socket, sql) do
      {:ok, [[_]]} -> true
      _ -> false
    end
  end

  defp setup_publication(socket, shard) do
    pub = pub_name(shard)
    with {:ok, _} <- SchemaCloner.simple_query(socket, "DROP PUBLICATION IF EXISTS #{pub}"),
         {:ok, rows} <- SchemaCloner.simple_query(socket,
           "SELECT tablename FROM pg_tables WHERE schemaname = '#{escape(shard)}' ORDER BY tablename"),
         [_ | _] <- rows,
         table_list = rows |> Enum.map(fn [t] -> ~s("#{shard}"."#{t}") end) |> Enum.join(", "),
         {:ok, _} <- SchemaCloner.simple_query(socket,
           "CREATE PUBLICATION #{pub} FOR TABLE #{table_list}") do
      Logger.info("[Migration] Publication #{pub} created (#{length(rows)} table(s))")
      :ok
    else
      [] -> {:error, "no tables found in schema #{shard}"}
      err -> err
    end
  end

  defp setup_subscription(target_socket, shard, source_cfg) do
    sub = sub_name(shard)
    pub = pub_name(shard)
    connstr = db_connstr(source_cfg)

    SchemaCloner.simple_query(target_socket, "ALTER SUBSCRIPTION #{sub} DISABLE")
    SchemaCloner.simple_query(target_socket, "ALTER SUBSCRIPTION #{sub} SET (slot_name = NONE)")
    SchemaCloner.simple_query(target_socket, "DROP SUBSCRIPTION IF EXISTS #{sub}")

    with {:ok, src} <- Connection.connect(source_cfg) do
      SchemaCloner.simple_query(src, "SELECT pg_drop_replication_slot('#{sub}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '#{sub}')")
      :gen_tcp.close(src)
    end

    sql = """
    CREATE SUBSCRIPTION #{sub}
      CONNECTION '#{connstr}'
      PUBLICATION #{pub}
    """
    case SchemaCloner.simple_query(target_socket, sql) do
      {:ok, _} ->
        Logger.info("[Migration] Subscription #{sub} created")
        :ok
      err -> err
    end
  end

  defp query_lag(source_socket, shard) do
    slot = sub_name(shard)
    sql = """
    SELECT CASE WHEN confirmed_flush_lsn IS NULL THEN NULL
                ELSE pg_current_wal_lsn() - confirmed_flush_lsn END
    FROM pg_replication_slots WHERE slot_name = '#{slot}'
    """
    case SchemaCloner.simple_query(source_socket, sql) do
      {:ok, [[nil]]} -> nil
      {:ok, [[lag]]} -> String.to_integer(lag)
      _ -> nil
    end
  end

  defp cleanup_replication(%{source_socket: src, target_socket: tgt, shard: shard}) do
    sub = sub_name(shard)
    pub = pub_name(shard)
    SchemaCloner.simple_query(tgt, "ALTER SUBSCRIPTION #{sub} DISABLE")
    SchemaCloner.simple_query(tgt, "ALTER SUBSCRIPTION #{sub} SET (slot_name = NONE)")
    SchemaCloner.simple_query(tgt, "DROP SUBSCRIPTION IF EXISTS #{sub}")
    SchemaCloner.simple_query(src, "SELECT pg_drop_replication_slot('#{sub}') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '#{sub}')")
    SchemaCloner.simple_query(src, "DROP PUBLICATION IF EXISTS #{pub}")
    :ok
  end

  defp drop_source_schema(%{source_socket: src, shard: shard}) do
    case SchemaCloner.simple_query(src, "DROP SCHEMA IF EXISTS \"#{shard}\" CASCADE") do
      {:ok, _} -> Logger.info("[Migration] Dropped source schema #{shard}")
      {:error, r} -> Logger.error("[Migration] Failed to drop source schema #{shard}: #{inspect(r)}")
    end
  end

  defp do_cancel(state) do
    if state.phase != :preparing do
      ShardRoute.set_status(state.shard, :open)
      ShardRoute.notify_waiters(state.shard)
      cleanup_replication(state)
    end
    AdminChannel.broadcast_migration_event(state.shard, "cancelled", nil)
    Logger.info("[Migration] #{state.shard} cancelled")
    close_sockets(state)
    Horde.DynamicSupervisor.terminate_child(Instashard.Migration.Supervisor, self())
    state
  end

  defp close_sockets(state) do
    if state.source_socket, do: :gen_tcp.close(state.source_socket)
    if state.target_socket, do: :gen_tcp.close(state.target_socket)
  end

  defp db_connstr(%{host: h, port: p, username: u, password: pw, database: db}) do
    "host=#{h} port=#{p} dbname=#{db} user=#{u} password=#{pw}"
  end

  defp pub_name(shard), do: "#{shard}#{@pub_suffix}"
  defp sub_name(shard), do: "#{shard}#{@sub_suffix}"
  defp escape(str), do: String.replace(str, "'", "''")
end
