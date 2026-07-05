defmodule Instashard.Backend.MigrationGate do
  @moduledoc """
  Mnesia-backed per-shard migration gate. Controls whether Pool.checkout
  is allowed for a given shard.

  States:
    :open     — normal operation (default)
    :closing  — migration draining; no new checkouts, wait for pool to refill
    :closed   — all in-flight tx done; ready for cutover

  Table schema: {:instashard_migration_gate, shard, status}
  ram_copies only.
  """

  require Logger

  @table :instashard_migration_gate

  @dialyzer {:nowarn_function, init: 0, status: 1, set_status: 2, open?: 1, notify_waiters: 1}

  def init do
    case :mnesia.create_table(@table,
      attributes: [:shard, :status],
      ram_copies: [node()],
      type: :set
    ) do
      {:atomic, :ok} -> :ok
      {:aborted, {:already_exists, @table}} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc "Returns the gate status for a shard: :open | :closing | :closed. Defaults to :open."
  def status(shard) do
    case :mnesia.dirty_read(@table, shard) do
      [{@table, ^shard, status}] -> status
      [] -> :open
    end
  end

  @doc "Set gate status for a shard."
  def set_status(shard, :open) do
    {:atomic, :ok} = :mnesia.sync_transaction(fn -> :mnesia.write({@table, shard, :open}) end)
    Logger.info("[MigrationGate] #{shard} → open")
    :ok
  end

  def set_status(shard, status) when status in [:closing, :closed] do
    :mnesia.dirty_write({@table, shard, status})
    Logger.info("[MigrationGate] #{shard} → #{status}")
    :ok
  end

  @doc "Returns true if checkout is allowed."
  def open?(shard) do
    status(shard) == :open
  end

  @doc "Broadcast gate open to all waiting sessions across all nodes."
  def notify_waiters(shard) do
    Phoenix.PubSub.broadcast(Instashard.PubSub, "gate:#{shard}", {:gate_open, shard})
  end
end
