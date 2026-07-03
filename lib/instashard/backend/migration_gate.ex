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
  # Local ETS: {shard, pid} bag — sessions waiting for a gate to reopen.
  @waiters :instashard_migration_waiters

  @dialyzer {:nowarn_function, init: 0, status: 1, set_status: 2, open?: 1,
             register_waiting: 2, notify_waiters: 1, notify_local_waiters: 1}

  def init do
    :ets.new(@waiters, [:public, :bag, :named_table])

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
  def set_status(shard, status) when status in [:open, :closing, :closed] do
    :mnesia.dirty_write({@table, shard, status})
    Logger.info("[MigrationGate] #{shard} → #{status}")
    :ok
  end

  @doc "Returns true if checkout is allowed."
  def open?(shard) do
    status(shard) == :open
  end

  @doc "Register a session pid as waiting for this shard's gate to reopen."
  def register_waiting(shard, pid) do
    :ets.insert(@waiters, {shard, pid})
  end

  @doc "Notify waiting sessions on all nodes that the gate is open again."
  def notify_waiters(shard) do
    nodes = [node() | Node.list()]
    :erpc.multicall(nodes, __MODULE__, :notify_local_waiters, [shard])
    :ok
  end

  @doc "Notify waiting sessions on this node only. Called via rpc.multicall."
  def notify_local_waiters(shard) do
    pids = :ets.lookup(@waiters, shard) |> Enum.map(fn {_, pid} -> pid end)
    :ets.delete(@waiters, shard)
    Enum.each(pids, fn pid ->
      Instashard.Proxy.ClientSession.migration_resumed(pid, shard)
    end)
    Logger.info("[MigrationGate] Notified #{length(pids)} waiting session(s) for #{shard} on #{node()}")
    :ok
  end
end
