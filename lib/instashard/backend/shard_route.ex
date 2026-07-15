defmodule Instashard.Backend.ShardRoute do
  @moduledoc """
  Mnesia-backed shard → {status, db_id} routing table.

  Merges the old MigrationGate + ShardMapping tables into one row per shard
  so Pool.checkout needs a single dirty_read instead of two.

  States:
    :open     — normal operation (default)
    :closing  — migration draining; no new checkouts, wait for pool to refill
    :closed   — all in-flight tx done; ready for cutover

  Table schema: {:instashard_shard_route, shard, status, db_id}
  ram_copies only — seeded from db/shards.json at startup.
  """

  require Logger

  @table :instashard_shard_route

  @dialyzer {:nowarn_function,
             init: 0, put: 2, route: 1, lookup: 1, status: 1, set_status: 2, open?: 1, all: 0}

  def init do
    :mnesia.start()

    case :mnesia.create_table(@table,
      attributes: [:shard, :status, :db_id],
      ram_copies: [node()],
      type: :set
    ) do
      {:atomic, :ok} ->
        Logger.debug("[ShardRoute] Table created")
        :ok
      {:aborted, {:already_exists, @table}} ->
        :ok
      {:aborted, reason} ->
        {:error, reason}
    end
  end

  @doc "Insert or update the db_id for a shard. Preserves existing gate status; defaults new shards to :open."
  def put(shard, db_id) when is_binary(db_id) do
    status =
      case :mnesia.dirty_read(@table, shard) do
        [{@table, ^shard, status, _}] -> status
        [] -> :open
      end

    :mnesia.dirty_write({@table, shard, status, db_id})
  end

  @doc """
  Single-read lookup for Pool.checkout. Returns {:ok, status, db_id} or {:error, :not_found}.
  """
  def route(shard) do
    case :mnesia.dirty_read(@table, shard) do
      [{@table, ^shard, status, db_id}] -> {:ok, status, db_id}
      [] -> {:error, :not_found}
    end
  end

  @doc "Look up db_id for a shard. Returns {:ok, db_id} or {:error, :not_found}."
  def lookup(shard) do
    case route(shard) do
      {:ok, _status, db_id} -> {:ok, db_id}
      err -> err
    end
  end

  @doc "Returns the gate status for a shard: :open | :closing | :closed. Defaults to :open."
  def status(shard) do
    case :mnesia.dirty_read(@table, shard) do
      [{@table, ^shard, status, _}] -> status
      [] -> :open
    end
  end

  @doc "Returns true if checkout is allowed."
  def open?(shard), do: status(shard) == :open

  @doc "Set gate status for a shard, preserving its db_id."
  def set_status(shard, :open) do
    {:atomic, :ok} =
      :mnesia.sync_transaction(fn ->
        db_id =
          case :mnesia.read(@table, shard) do
            [{@table, ^shard, _status, db_id}] -> db_id
            [] -> nil
          end

        :mnesia.write({@table, shard, :open, db_id})
      end)

    Logger.info("[ShardRoute] #{shard} → open")
    :ok
  end

  def set_status(shard, status) when status in [:closing, :closed] do
    db_id =
      case :mnesia.dirty_read(@table, shard) do
        [{@table, ^shard, _status, db_id}] -> db_id
        [] -> nil
      end

    :mnesia.dirty_write({@table, shard, status, db_id})
    Logger.info("[ShardRoute] #{shard} → #{status}")
    :ok
  end

  @doc "Return all {shard, db_id} pairs."
  def all do
    :mnesia.dirty_match_object({@table, :_, :_, :_})
    |> Enum.map(fn {@table, shard, _status, db_id} -> {shard, db_id} end)
  end

  @doc "Broadcast gate open to all waiting sessions across all nodes."
  def notify_waiters(shard) do
    Phoenix.PubSub.broadcast(Instashard.PubSub, "gate:#{shard}", {:gate_open, shard})
  end
end
