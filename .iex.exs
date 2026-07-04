alias Instashard.Backend.{ConfigStore, DbRegistry, Manager, Migration, MigrationGate, Pool, ShardMapping}

defmodule DB do
  def list,                      do: DbRegistry.all()
  def get(id),                   do: DbRegistry.get(id)
  def pool(id),                  do: Pool.count(id)
  def shards,                    do: ShardMapping.all()

  def add(id, host, port, user, pass, db, pool_size \\ 20) do
    cfg = %{host: host, port: port, username: user, password: pass, database: db, pool_size: pool_size}
    with :ok <- DbRegistry.put(id, cfg),
         :ok <- ConfigStore.persist_databases() do
      :ok
    end
  end

  def remove(id) do
    with :ok <- DbRegistry.delete(id),
         :ok <- ConfigStore.persist_databases() do
      :ok
    end
  end

  def set_pool_size(id, n), do: Manager.update_pool_size(id, n)
  def reload,               do: ConfigStore.load_databases()
end

defmodule M do
  def migrate(shard, db), do: Migration.start_migration(shard, db)
  def status(shard),      do: Migration.status(shard)
  def drain(shard),       do: Migration.drain(shard)
  def cutover(shard),     do: Migration.cutover(shard)
  def cancel(shard),      do: Migration.cancel(shard)

  def gate(shard),        do: MigrationGate.status(shard)
  def active_tx(shard),   do: Pool.active_tx_count(shard)
end
