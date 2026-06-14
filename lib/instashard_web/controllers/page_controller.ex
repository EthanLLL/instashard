defmodule InstashardWeb.PageController do
  use InstashardWeb, :controller

  def home(conn, _params) do
    render(conn, :home)
  end
end
