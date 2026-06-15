defmodule InstashardWeb.Router do
  use InstashardWeb, :router

  pipeline :api do
    plug :accepts, ["json"]
  end

  scope "/api", InstashardWeb do
    pipe_through :api
  end
end
