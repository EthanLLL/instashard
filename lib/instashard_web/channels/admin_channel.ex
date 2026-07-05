defmodule InstashardWeb.AdminChannel do
  use Phoenix.Channel
  require Logger

  alias Instashard.Backend.{DbRegistry, MigrationGate, Pool, ShardMapping}
  alias Instashard.Migration.{Supervisor, Worker}

  @impl true
  def join("admin:dashboard", _params, socket) do
    :timer.send_interval(2_000, :push_snapshot)
    {:ok, socket}
  end

  def join("admin:migration", _params, socket) do
    :timer.send_interval(2_000, :push_migration_status)
    {:ok, socket}
  end

  def join(topic, _params, _socket) do
    {:error, %{reason: "unknown topic #{topic}"}}
  end

  # ── Dashboard ──────────────────────────────────────────────────────────

  @impl true
  def handle_in("request:snapshot", _params, socket) do
    push(socket, "state:snapshot", dashboard_snapshot())
    {:noreply, socket}
  end

  # ── Database commands ──────────────────────────────────────────────────

  def handle_in("db:add", params, socket) do
    %{"id" => id, "host" => host, "port" => port, "username" => username,
      "password" => password, "database" => database, "pool_size" => pool_size} = params
    cfg = %{host: host, port: port, username: username,
            password: password, database: database, pool_size: pool_size}
    case DbRegistry.get(id) do
      {:ok, _} ->
        push(socket, "db:error", %{id: id, reason: "already exists"})
      {:error, :not_found} ->
        DbRegistry.put(id, cfg)
        Instashard.Backend.ConfigStore.persist_databases()
        Instashard.Backend.Manager.replenish(id)
        push(socket, "db:ok", %{id: id})
    end
    {:noreply, socket}
  end

  def handle_in("db:set_pool_size", %{"id" => id, "pool_size" => pool_size}, socket) do
    case Instashard.Backend.Manager.update_pool_size(id, pool_size) do
      :ok -> push(socket, "db:ok", %{id: id})
      {:error, reason} -> push(socket, "db:error", %{id: id, reason: inspect(reason)})
    end
    {:noreply, socket}
  end

  # ── Migration commands ─────────────────────────────────────────────────

  def handle_in("migration:start", %{"shard" => shard, "target_db" => target_db}, socket) do
    case Supervisor.start_worker(shard, target_db) do
      :ok -> {:noreply, socket}
      {:error, reason} ->
        broadcast_migration_event(shard, "error", inspect(reason))
        {:noreply, socket}
    end
  end

  def handle_in("migration:drain", %{"shard" => shard}, socket) do
    case Worker.drain(shard) do
      :ok -> {:noreply, socket}
      {:error, reason} ->
        broadcast_migration_event(shard, "error", inspect(reason))
        {:noreply, socket}
    end
  end

  def handle_in("migration:cutover", %{"shard" => shard}, socket) do
    case Worker.cutover(shard) do
      :ok -> {:noreply, socket}
      {:error, reason} ->
        broadcast_migration_event(shard, "error", inspect(reason))
        {:noreply, socket}
    end
  end

  def handle_in("migration:cancel", %{"shard" => shard}, socket) do
    case Worker.cancel(shard) do
      :ok -> {:noreply, socket}
      {:error, reason} ->
        broadcast_migration_event(shard, "error", inspect(reason))
        {:noreply, socket}
    end
  end

  # ── PubSub / timer ────────────────────────────────────────────────────

  @impl true
  def handle_info(:push_snapshot, socket) do
    push(socket, "state:update", dashboard_snapshot())
    {:noreply, socket}
  end

  def handle_info(:push_migration_status, socket) do
    push(socket, "migration:status", %{migrations: Supervisor.all_statuses()})
    {:noreply, socket}
  end

  def handle_info({:migration_event, payload}, socket) do
    push(socket, "migration:event", payload)
    {:noreply, socket}
  end

  def handle_info({:state_update, payload}, socket) do
    push(socket, "state:update", payload)
    {:noreply, socket}
  end

  # ── Public broadcast helpers ───────────────────────────────────────────

  def broadcast_migration_event(shard, status, detail) do
    Phoenix.PubSub.broadcast(Instashard.PubSub, "admin:migration", {:migration_event, %{
      shard: shard, status: status, detail: detail,
      ts: System.system_time(:millisecond)
    }})
  end

  def broadcast_state_update do
    Phoenix.PubSub.broadcast(Instashard.PubSub, "admin:dashboard", {:state_update, dashboard_snapshot()})
  end

  # ── Helpers ───────────────────────────────────────────────────────────

  defp dashboard_snapshot do
    dbs = DbRegistry.all() |> Enum.map(fn {id, cfg} ->
      %{id: id, host: cfg.host, port: cfg.port, pool_size: cfg.pool_size, idle: Pool.count(id)}
    end)
    shards = ShardMapping.all() |> Enum.map(fn {shard, db_id} ->
      %{shard: shard, db_id: db_id,
        active_tx: Pool.active_tx_count(shard),
        gate: MigrationGate.status(shard) |> to_string()}
    end)
    %{dbs: dbs, shards: shards}
  end
end
