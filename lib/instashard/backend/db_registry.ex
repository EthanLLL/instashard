defmodule Instashard.Backend.DbRegistry do
  @moduledoc """
  Mnesia-backed registry of physical database configurations.
  Maps db_id (string) → connection config map.

  Table schema: {:instashard_db_registry, db_id, cfg}
  ram_copies only — seeded from db/databases.json at startup.
  """

  require Logger

  @table :instashard_db_registry

  @dialyzer {:nowarn_function, init: 0, put: 2, get: 1, all: 0, delete: 1}

  def init do
    case :mnesia.create_table(@table,
      attributes: [:db_id, :cfg],
      ram_copies: [node()],
      type: :set
    ) do
      {:atomic, :ok} ->
        Logger.debug("[DbRegistry] Table created")
        :ok
      {:aborted, {:already_exists, @table}} ->
        :ok
      {:aborted, reason} ->
        {:error, reason}
    end
  end

  @doc "Insert or update a db_id → cfg entry. No-op if db_id already exists (seed semantics)."
  def put_new(db_id, cfg) when is_binary(db_id) do
    case :mnesia.dirty_read(@table, db_id) do
      [] -> :mnesia.dirty_write({@table, db_id, cfg})
      _  -> :ok
    end
  end

  @doc "Insert or overwrite a db_id → cfg entry (runtime API semantics)."
  def put(db_id, cfg) when is_binary(db_id) do
    :mnesia.dirty_write({@table, db_id, cfg})
  end

  @doc "Look up cfg for a db_id. Returns {:ok, cfg} or {:error, :not_found}."
  def get(db_id) when is_binary(db_id) do
    case :mnesia.dirty_read(@table, db_id) do
      [{@table, ^db_id, cfg}] -> {:ok, cfg}
      [] -> {:error, :not_found}
    end
  end

  @doc "Return all {db_id, cfg} pairs."
  def all do
    :mnesia.dirty_match_object({@table, :_, :_})
    |> Enum.map(fn {@table, db_id, cfg} -> {db_id, cfg} end)
  end

  @doc "Remove a db entry."
  def delete(db_id) when is_binary(db_id) do
    :mnesia.dirty_delete(@table, db_id)
  end
end
