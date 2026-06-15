defmodule InstashardWeb do
  @moduledoc """
  The entrypoint for defining your web interface, such
  as controllers, channels, and so on.

  This can be used in your application as:

      use InstashardWeb, :controller
      use InstashardWeb, :channel
  """

  def static_paths, do: ~w()

  def router do
    quote do
      use Phoenix.Router, helpers: false

      import Plug.Conn
      import Phoenix.Controller
      import Phoenix.LiveView.Router
    end
  end

  def channel do
    quote do
      use Phoenix.Channel
    end
  end

  def controller do
    quote do
      use Phoenix.Controller, formats: [:json]

      import Plug.Conn

      unquote(verified_routes())
    end
  end

  def verified_routes do
    quote do
      use Phoenix.VerifiedRoutes,
        endpoint: InstashardWeb.Endpoint,
        router: InstashardWeb.Router,
        statics: InstashardWeb.static_paths()
    end
  end

  @doc """
  When used, dispatch to the appropriate controller/channel/etc.
  """
  defmacro __using__(which) when is_atom(which) do
    apply(__MODULE__, which, [])
  end
end
