defmodule LassoWeb.Plugs.TracedRouter do
  @moduledoc """
  Wraps HTTP routing in an optional server span with exception-safe cleanup.
  Body parsing precedes this span. Health checks, metrics and dashboard requests
  are excluded. The disabled path calls the router directly.
  """

  @behaviour Plug

  @impl true
  def init(opts), do: LassoWeb.Router.init(opts)

  @impl true
  def call(conn, opts) do
    if Lasso.Observability.Tracing.enabled?() and rpc_path?(conn.path_info) do
      Lasso.Observability.Tracing.http(conn, &LassoWeb.Router.call(&1, opts))
    else
      LassoWeb.Router.call(conn, opts)
    end
  end

  defp rpc_path?(["rpc" | _]), do: true
  defp rpc_path?(_), do: false
end
