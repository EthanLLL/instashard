defmodule Instashard.Backend.Pool do
  @moduledoc """
  ETS-backed connection pool. Checkout/checkin are lock-free via ets:take.
  The Manager GenServer only replenishes connections — it is off the hot path.

  Table schema: ordered_set, key = {shard_name, ref}, value = socket
  Using a unique ref per entry lets ets:take remove exactly one connection atomically.
  """

  @table :instashard_pool

  def init do
    :ets.new(@table, [:public, :ordered_set, :named_table, read_concurrency: true, write_concurrency: true])
  end

  @doc "Return a socket for the given shard, or {:error, :empty}."
  def checkout(shard) do
    # Select one ref for this shard, then atomically take it.
    # If another process took it between select and take, retry.
    ms = [{{{shard, :"$1"}, :_}, [], [:"$1"]}]

    case :ets.select(@table, ms, 1) do
      {[ref | _], _cont} ->
        case :ets.take(@table, {shard, ref}) do
          [{_, socket}] -> {:ok, socket}
          [] -> checkout(shard)
        end

      :"$end_of_table" ->
        {:error, :empty}
    end
  end

  @doc "Return a socket to the pool."
  def checkin(shard, socket) do
    :ets.insert(@table, {{shard, make_ref()}, socket})
    :ok
  end

  @doc "Count idle connections for a shard."
  def count(shard) do
    :ets.select_count(@table, [{{{shard, :_}, :_}, [], [true]}])
  end
end
