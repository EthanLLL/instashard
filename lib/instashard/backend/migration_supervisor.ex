defmodule Instashard.Migration.Supervisor do
  use Horde.DynamicSupervisor

  def start_link(opts) do
    Horde.DynamicSupervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    Horde.DynamicSupervisor.init(strategy: :one_for_one, members: :auto)
  end

  @doc "Start a migration worker for a shard. Returns {:error, :already_migrating} if one exists."
  def start_worker(shard, target_db_id) do
    case Horde.Registry.lookup(Instashard.Migration.Registry, shard) do
      [{_pid, _}] ->
        {:error, :already_migrating}
      [] ->
        spec = {Instashard.Migration.Worker, shard: shard, target_db_id: target_db_id}
        case Horde.DynamicSupervisor.start_child(__MODULE__, spec) do
          {:ok, _pid} -> :ok
          {:ok, _pid, _info} -> :ok
          {:error, reason} -> {:error, reason}
        end
    end
  end

  @doc "List all active migration statuses across all workers."
  def all_statuses do
    Horde.Registry.select(Instashard.Migration.Registry, [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.map(fn {_shard, pid} ->
      try do
        GenServer.call(pid, :status, 3_000)
      catch
        :exit, _ -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  @doc "Look up the pid for a shard's migration worker."
  def worker_pid(shard) do
    case Horde.Registry.lookup(Instashard.Migration.Registry, shard) do
      [{pid, _}] -> {:ok, pid}
      [] -> {:error, :not_found}
    end
  end
end

defmodule Instashard.Migration.Registry do
  def child_spec(_opts) do
    Horde.Registry.child_spec(
      keys: :unique,
      name: __MODULE__,
      members: :auto
    )
  end
end
