defmodule Instashard.Proxy.Listener do
  @moduledoc """
  Listens for incoming client TCP connections and spawns a supervised
  ClientSession GenServer for each one.
  """

  use GenServer
  require Logger

  def start_link(opts) do
    port = Keyword.get(opts, :port, 5400)
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    case :gen_tcp.listen(port, [:binary, packet: 0, active: false, reuseaddr: true]) do
      {:ok, listen_socket} ->
        Logger.info("[Listener] Listening on port #{port}")
        send(self(), :accept)
        {:ok, %{listen_socket: listen_socket}}

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info(:accept, %{listen_socket: listen_socket} = state) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        Logger.info("[Listener] New client connection")

        case Instashard.Proxy.SessionSupervisor.start_session(client_socket) do
          {:ok, pid} ->
            :gen_tcp.controlling_process(client_socket, pid)
          {:error, reason} ->
            Logger.error("[Listener] Failed to start session: #{inspect(reason)}")
            :gen_tcp.close(client_socket)
        end

      {:error, reason} ->
        Logger.error("[Listener] Accept error: #{inspect(reason)}")
    end

    send(self(), :accept)
    {:noreply, state}
  end
end
