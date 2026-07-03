alias Instashard.Backend.{Migration, MigrationGate, Pool, ShardMapping}

defmodule M do
  def migrate(shard, db), do: Migration.start_migration(shard, db)
  def status(shard),      do: Migration.status(shard)
  def drain(shard),       do: Migration.drain(shard)
  def cutover(shard),     do: Migration.cutover(shard)
  def cancel(shard),      do: Migration.cancel(shard)

  def shards,             do: ShardMapping.all()
  def pool(shard),        do: Pool.count(shard)
  def gate(shard),        do: MigrationGate.status(shard)
end
