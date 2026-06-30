defmodule Instashard.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      InstashardWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:instashard, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: Instashard.PubSub},
      InstashardWeb.Endpoint,
      Instashard.Backend.Manager,
      Instashard.Proxy.SessionSupervisor,
      {Instashard.Proxy.Listener, port: 5400}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Instashard.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    InstashardWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
