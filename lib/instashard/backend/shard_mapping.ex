defmodule Instashard.Backend.ShardMapping do
  @moduledoc """
  Mnesia-backed shard → db_id mapping.
  Stores only the db_id string; callers that need the full cfg
  do a second lookup in DbRegistry.

  Table schema: {:instashard_shard_map, shard_name, db_id}
  ram_copies only — seeded from db/shards.json at startup.
  """

  require Logger

  @table :instashard_shard_map

  @dialyzer {:nowarn_function, init: 0, put: 2, lookup: 1, all: 0, delete: 1}

  def init do
    :mnesia.start()

    case :mnesia.create_table(@table,
      attributes: [:shard, :db_id],
      ram_copies: [node()],
      type: :set
    ) do
      {:atomic, :ok} ->
        Logger.debug("[ShardMapping] Table created")
        :ok
      {:aborted, {:already_exists, @table}} ->
        :ok
      {:aborted, reason} ->
        {:error, reason}
    end
  end

  @doc "Insert or update a shard → db_id entry."
  def put(shard, db_id) when is_binary(db_id) do
    :mnesia.dirty_write({@table, shard, db_id})
  end

  @doc "Look up db_id for a shard. Returns {:ok, db_id} or {:error, :not_found}."
  def lookup(shard) do
    case :mnesia.dirty_read(@table, shard) do
      [{@table, ^shard, db_id}] -> {:ok, db_id}
      [] -> {:error, :not_found}
    end
  end

  @doc "Return all {shard, db_id} pairs."
  def all do
    :mnesia.dirty_match_object({@table, :_, :_})
    |> Enum.map(fn {@table, shard, db_id} -> {shard, db_id} end)
  end

  @doc "Remove a shard mapping."
  def delete(shard) do
    :mnesia.dirty_delete(@table, shard)
  end
end
