defmodule Instashard.Backend.StmtCache do
  @moduledoc """
  Global ETS cache mapping sql_hash → {internal_name, sql}.

  Internal names use the "is_" prefix (instashard) to avoid collisions with
  client-named statements. The hash is a truncated SHA-256 hex string.

  Lifecycle: populated on first Parse, never evicted — the cache maps SQL text
  to a canonical internal name. Per-socket tracking (stmt_set) is separate and
  lives in the pool ETS table.
  """

  @table :instashard_stmt_cache
  @name_prefix "is_"

  def init do
    :ets.new(@table, [:public, :set, :named_table, read_concurrency: true])
  end

  @doc """
  Look up or create an internal statement name for the given SQL.
  Returns {internal_name, :hit | :miss}.
  """
  def get_or_create(sql) do
    hash = sql_hash(sql)

    case :ets.lookup(@table, hash) do
      [{^hash, {internal_name, _sql}}] ->
        {internal_name, :hit}

      [] ->
        internal_name = @name_prefix <> hash
        :ets.insert_new(@table, {hash, {internal_name, sql}})
        # Re-lookup in case of concurrent insert race — accept whatever landed first
        [{^hash, {actual_name, _}}] = :ets.lookup(@table, hash)
        {actual_name, :miss}
    end
  end

  @doc "Look up an internal name by sql_hash. Returns nil if not found."
  def lookup_by_hash(hash) do
    case :ets.lookup(@table, hash) do
      [{^hash, {internal_name, _sql}}] -> internal_name
      [] -> nil
    end
  end

  @doc "Look up the SQL for an internal_name. Returns nil if not found."
  def lookup_sql(internal_name) do
    hash = String.replace_prefix(internal_name, @name_prefix, "")
    case :ets.lookup(@table, hash) do
      [{^hash, {_name, sql}}] -> sql
      [] -> nil
    end
  end

  @doc "Compute the cache key for a SQL string."
  def sql_hash(sql) do
    :crypto.hash(:sha256, sql)
    |> Base.encode16(case: :lower)
    |> binary_part(0, 16)
  end
end
