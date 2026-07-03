defmodule Instashard.Backend.ShardMapping do
  @moduledoc """
  Mnesia-backed shard→db_config mapping. ram_copies only — no disk state.
  On startup the Manager seeds the initial mapping; runtime updates take effect
  immediately on all nodes without restart.

  Table schema: {:instashard_shard_map, shard_name, db_config}
  """

  require Logger

  @table :instashard_shard_map

  # :mnesia is an OTP application — available at runtime but not at compile time.
  @dialyzer {:nowarn_function, init: 0, put: 2, lookup: 1, all: 0, delete: 1}

  def init do
    :mnesia.start()

    case :mnesia.create_table(@table,
      attributes: [:shard, :db_config],
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

  @doc "Insert or update a shard→db_config entry."
  def put(shard, db_config) do
    :mnesia.dirty_write({@table, shard, db_config})
  end

  @doc "Look up db_config for a shard. Returns {:ok, cfg} or {:error, :not_found}."
  def lookup(shard) do
    case :mnesia.dirty_read(@table, shard) do
      [{@table, ^shard, db_config}] -> {:ok, db_config}
      [] -> {:error, :not_found}
    end
  end

  @doc "Return all {shard, db_config} pairs."
  def all do
    :mnesia.dirty_match_object({@table, :_, :_})
    |> Enum.map(fn {@table, shard, db_config} -> {shard, db_config} end)
  end

  @doc "Remove a shard mapping."
  def delete(shard) do
    :mnesia.dirty_delete(@table, shard)
  end
end
