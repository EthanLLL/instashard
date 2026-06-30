defmodule Instashard.Proxy.SessionSupervisor do
  use DynamicSupervisor

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  def start_session(client_socket) do
    DynamicSupervisor.start_child(__MODULE__, {Instashard.Proxy.ClientSession, client_socket})
  end
end
