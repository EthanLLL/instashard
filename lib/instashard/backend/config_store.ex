defmodule Instashard.Backend.ConfigStore do
  @moduledoc """
  Reads and writes the two seed files: db/databases.json and db/shards.json.

  Seed (startup):
    load_databases/0  — reads databases.json, returns [cfg_map]
    load_shards/0     — reads shards.json, returns [{shard, db_id}]

  Persist (runtime, after Mnesia write):
    persist_databases/0  — dumps DbRegistry → databases.json
    persist_shards/0     — dumps ShardMapping → shards.json

  Writes are atomic: write to a tmp file, then rename.
  """

  alias Instashard.Backend.{DbRegistry, ShardMapping}

  @databases_file "db/databases.json"
  @shards_file "db/shards.json"

  # ── Seed readers ─────────────────────────────────────────────────────

  @doc "Returns {:ok, [%{id, host, port, username, password, database}]} or {:error, reason}."
  def load_databases do
    with {:ok, raw} <- File.read(@databases_file),
         {:ok, %{"databases" => list}} <- Jason.decode(raw) do
      dbs = Enum.map(list, &decode_db/1)
      {:ok, dbs}
    end
  end

  @doc "Returns {:ok, [{shard_name, db_id}]} or {:error, reason}."
  def load_shards do
    with {:ok, raw} <- File.read(@shards_file),
         {:ok, map} <- Jason.decode(raw) do
      pairs =
        Enum.flat_map(map, fn {db_id, shards} ->
          Enum.map(shards, fn shard -> {shard, db_id} end)
        end)
      {:ok, pairs}
    end
  end

  # ── Persist writers ───────────────────────────────────────────────────

  @doc "Dump current DbRegistry to databases.json. Atomic write."
  def persist_databases do
    entries =
      DbRegistry.all()
      |> Enum.map(fn {db_id, cfg} -> encode_db(db_id, cfg) end)

    payload = Jason.encode!(%{"databases" => entries}, pretty: true)
    atomic_write(@databases_file, payload)
  end

  @doc "Dump current ShardMapping to shards.json in {db_id => [shard, ...]} format. Atomic write."
  def persist_shards do
    by_db =
      ShardMapping.all()
      |> Enum.group_by(fn {_shard, db_id} -> db_id end, fn {shard, _} -> shard end)
      |> Enum.map(fn {db_id, shards} -> {db_id, Enum.sort(shards)} end)
      |> Map.new()

    payload = Jason.encode!(by_db, pretty: true)
    atomic_write(@shards_file, payload)
  end

  # ── Private ───────────────────────────────────────────────────────────

  defp decode_db(map) do
    %{
      id:        map["id"],
      host:      map["host"],
      port:      map["port"],
      username:  map["username"],
      password:  map["password"],
      database:  map["database"],
      pool_size: map["pool_size"] || 20
    }
  end

  defp encode_db(db_id, cfg) do
    %{
      "id"        => db_id,
      "host"      => cfg.host,
      "port"      => cfg.port,
      "username"  => cfg.username,
      "password"  => cfg.password,
      "database"  => cfg.database,
      "pool_size" => cfg.pool_size
    }
  end

  defp atomic_write(path, contents) do
    tmp = path <> ".tmp"
    with :ok <- File.write(tmp, contents),
         :ok <- File.rename(tmp, path) do
      :ok
    end
  end
end
