defmodule Instashard.Backend.Pool do
  @moduledoc """
  ETS-backed connection pool. Checkout/checkin are lock-free via ets:take.
  The Manager GenServer only replenishes connections — it is off the hot path.

  Table schema: ordered_set, key = {shard_name, ref}
  Value: {socket, parse_count, stmt_set}
    - parse_count  integer   how many real Parse messages have been sent on this socket
    - stmt_set     MapSet    internal statement names already prepared on this socket

  When checkin is called with parse_count > @replace_threshold, the socket is
  closed and Manager is asked to replenish rather than returning it to the pool.
  """

  require Logger

  @table :instashard_pool
  # Replace a socket after this many prepared statements to bound per-connection state.
  @replace_threshold 100

  def init do
    :ets.new(@table, [:public, :ordered_set, :named_table, read_concurrency: true, write_concurrency: true])
  end

  @doc "Return {socket, parse_count, stmt_set} for the given shard, or {:error, :empty}."
  def checkout(shard) do
    ms = [{{{shard, :"$1"}, :_}, [], [:"$1"]}]

    case :ets.select(@table, ms, 1) do
      {[ref | _], _cont} ->
        case :ets.take(@table, {shard, ref}) do
          [{_, entry}] -> {:ok, entry}
          [] -> checkout(shard)
        end

      :"$end_of_table" ->
        {:error, :empty}
    end
  end

  @doc """
  Return a connection entry to the pool.
  If parse_count exceeds the threshold, close the socket and ask Manager to replenish.
  """
  def checkin(shard, {socket, parse_count, _stmt_set} = entry) do
    if parse_count > @replace_threshold do
      Logger.info("[Pool] Replacing socket for #{shard} (parse_count=#{parse_count})")
      :gen_tcp.close(socket)
      Instashard.Backend.Manager.replenish(shard)
      :ok
    else
      :ets.insert(@table, {{shard, make_ref()}, entry})
      :ok
    end
  end

  @doc "Wrap a raw socket into a fresh pool entry (parse_count=0, stmt_set=empty)."
  def new_entry(socket), do: {socket, 0, MapSet.new()}

  @doc "Count idle connections for a shard."
  def count(shard) do
    :ets.select_count(@table, [{{{shard, :_}, :_}, [], [true]}])
  end
end
