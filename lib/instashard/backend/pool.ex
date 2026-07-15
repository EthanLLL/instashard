defmodule Instashard.Backend.Pool do
  @moduledoc """
  ETS-backed connection pool. Checkout/checkin are lock-free via ets:take.
  Connections are keyed by db_id, not shard — one pool per physical DB shared
  across all its shards.

  Pool table:  ordered_set, key = {db_id, ref}
               value = {socket, parse_count, stmt_set}

  Active-tx:   set, key = shard_name, value = integer
               Tracks in-flight transactions per shard for migration drain.

  Socket replacement: after @replace_threshold prepared statements the socket is
  closed and discarded via Manager.
  """

  require Logger

  @pool_table :instashard_pool
  @tx_table   :instashard_active_tx

  def init do
    :ets.new(@pool_table, [:public, :ordered_set, :named_table, read_concurrency: true, write_concurrency: true])
    :ets.new(@tx_table,   [:public, :set,          :named_table, read_concurrency: true, write_concurrency: true])
  end

  # ── Checkout ──────────────────────────────────────────────────────────

  @doc """
  Check out a connection for the given shard.
  Returns {:ok, db_id, entry} | {:error, :migrating} | {:error, :empty} | {:error, :no_mapping}
  On :empty the caller should schedule a retry with exponential backoff.
  """
  def checkout(shard) do
    case Instashard.Backend.ShardRoute.route(shard) do
      {:ok, :open, db_id} ->
        case do_checkout(db_id) do
          {:ok, entry} ->
            :ets.update_counter(@tx_table, shard, {2, 1}, {shard, 0})
            {:ok, db_id, entry}
          {:error, :empty} ->
            {:error, :empty}
        end
      {:ok, _closing_or_closed, _db_id} ->
        {:error, :migrating}
      {:error, :not_found} ->
        {:error, :no_mapping}
    end
  end

  defp do_checkout(db_id) do
    case :ets.next(@pool_table, {db_id, 0}) do
      {^db_id, _} = key ->
        case :ets.take(@pool_table, key) do
          [{_, entry}] -> {:ok, entry}
          [] -> do_checkout(db_id)
        end
      _ ->
        {:error, :empty}
    end
  end

  # ── Checkin ───────────────────────────────────────────────────────────

  @doc "Return a connection to the pool and decrement active-tx for the shard."
  def checkin(db_id, shard, entry) do
    decrement_active_tx(shard)
    :ets.insert(@pool_table, {{db_id, make_ref()}, entry})
    :ok
  end

  @doc "Decrement active-tx for a shard without returning the socket (called by session on backend error)."
  def decrement_tx(shard), do: decrement_active_tx(shard)

  # ── Pool management ───────────────────────────────────────────────────

  @doc "Wrap a raw socket into a fresh pool entry."
  def new_entry(socket), do: {socket, 0, MapSet.new()}

  @doc "Put a connection into the pool (Manager fill path, no active-tx change)."
  def put(db_id, entry) do
    :ets.insert(@pool_table, {{db_id, make_ref()}, entry})
    :ok
  end

  @doc "Count idle connections for a db_id."
  def count(db_id) do
    :ets.select_count(@pool_table, [{{{db_id, :_}, :_}, [], [true]}])
  end

  @doc "Count in-flight transactions for a shard."
  def active_tx_count(shard) do
    case :ets.lookup(@tx_table, shard) do
      [{_, n}] -> n
      []       -> 0
    end
  end

  @doc "Remove and close up to n idle connections for a db_id. Used when pool_size is reduced."
  def trim(db_id, n) when n > 0 do
    ms = [{{{db_id, :"$1"}, :_}, [], [:"$1"]}]
    case :ets.select(@pool_table, ms, n) do
      {refs, _cont} ->
        Enum.each(refs, fn ref ->
          case :ets.take(@pool_table, {db_id, ref}) do
            [{_, {socket, _, _}}] -> :gen_tcp.close(socket)
            [] -> :ok
          end
        end)
      :"$end_of_table" -> :ok
    end
    :ok
  end

  @doc "Remove and close all idle connections for a db_id."
  def flush(db_id) do
    entries = :ets.select(@pool_table, [{{{db_id, :_}, :"$1"}, [], [:"$1"]}])
    :ets.match_delete(@pool_table, {{db_id, :_}, :_})
    Enum.each(entries, fn {socket, _, _} -> :gen_tcp.close(socket) end)
    Logger.info("[Pool] Flushed #{length(entries)} idle connections for #{db_id}")
    :ok
  end

  # ── Private ───────────────────────────────────────────────────────────

  defp decrement_active_tx(shard) do
    :ets.update_counter(@tx_table, shard, {2, -1, 0, 0}, {shard, 0})
  end
end
