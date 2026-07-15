alias Instashard.Backend.{ConfigStore, DbRegistry, Manager, Pool, ShardRoute, StmtCache}
alias Instashard.Migration.{Supervisor, Worker}

defmodule DB do
  def list,                      do: DbRegistry.all()
  def get(id),                   do: DbRegistry.get(id)
  def pool(id),                  do: Pool.count(id)
  def shards,                    do: ShardRoute.all()

  def add(id, host, port, user, pass, db, pool_size \\ 20) do
    cfg = %{host: host, port: port, username: user, password: pass, database: db, pool_size: pool_size}
    with :ok <- DbRegistry.put(id, cfg),
         :ok <- ConfigStore.persist_databases() do
      Manager.add_db(id)
      :ok
    end
  end

  def remove(id) do
    with :ok <- DbRegistry.delete(id),
         :ok <- ConfigStore.persist_databases() do
      Pool.flush(id)
      :ok
    end
  end

  def set_pool_size(id, n), do: Manager.update_pool_size(id, n)
  def reload,               do: ConfigStore.load_databases()
end

defmodule M do
  def migrate(shard, db), do: Supervisor.start_worker(shard, db)
  def all,                do: Supervisor.all_statuses()
  def status(shard),      do: Worker.status(shard)
  def drain(shard),       do: Worker.drain(shard)
  def cutover(shard),     do: Worker.cutover(shard)
  def cancel(shard),      do: Worker.cancel(shard)

  def gate(shard),        do: ShardRoute.status(shard)
  def active_tx(shard),   do: Pool.active_tx_count(shard)
end
