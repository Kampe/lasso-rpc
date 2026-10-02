defmodule Lasso.TracingIntegrationTest do
  use Lasso.Test.LassoIntegrationCase

  @moduletag :integration
  require Record
  alias Lasso.RPC.{RequestOptions, RequestPipeline}

  Record.defrecordp(:span, Record.extract(:span, from_lib: "opentelemetry/include/otel_span.hrl"))

  setup do
    previous = Application.get_env(:lasso, :otel_enabled, false)
    Application.put_env(:lasso, :otel_enabled, true)
    :ok = :otel_batch_processor.set_exporter(:otel_exporter_pid, self())

    on_exit(fn ->
      Application.put_env(:lasso, :otel_enabled, previous)
      :otel_batch_processor.set_exporter(:none)
    end)

    :ok
  end

  test "real routing retains a successful parent and both failed and fallback attempts", %{
    chain: chain
  } do
    setup_providers([
      %{
        id: "failed",
        priority: 10,
        profile: "public",
        behavior: {:error, Lasso.JSONRPC.Error.new(-32_000, "archive node required")}
      },
      %{id: "fallback", priority: 20, profile: "public", behavior: :healthy}
    ])

    assert {:ok, _value, ctx} =
             RequestPipeline.execute_via_channels(chain, "eth_getLogs", [], %RequestOptions{
               profile: "public",
               strategy: :priority,
               timeout_ms: 1_000
             })

    assert ctx.retries == 1
    :otel_tracer_provider.force_flush()

    records =
      for _ <- 1..3 do
        assert_receive {:span, record}, 2_000
        record
      end

    request = Enum.find(records, &(span(&1, :name) == "lasso.rpc"))
    attempts = Enum.filter(records, &(span(&1, :name) == "lasso.upstream"))
    assert length(attempts) == 2
    assert Enum.all?(attempts, &(span(&1, :parent_span_id) == span(request, :span_id)))
    assert Enum.all?(records, &(span(&1, :trace_id) == span(request, :trace_id)))
    assert :otel_attributes.map(span(request, :attributes))["lasso.outcome"] == "success"
    assert :otel_attributes.map(span(request, :attributes))["lasso.attempts"] == 2
    providers = Enum.map(attempts, &:otel_attributes.map(span(&1, :attributes))["lasso.provider"])
    assert Enum.sort(providers) == ["failed", "fallback"]
  end

  test "HTTP ingress keeps incoming trace context through actual routing", %{chain: chain} do
    setup_providers([%{id: "healthy", profile: "public", behavior: :healthy}])
    {:ok, _} = Application.ensure_all_started(:inets)
    port = LassoWeb.Endpoint.config(:http)[:port]
    url = String.to_charlist("http://127.0.0.1:#{port}/rpc/#{chain}")
    trace_id = String.duplicate("a", 32)
    headers = [{~c"traceparent", String.to_charlist("00-#{trace_id}-bbbbbbbbbbbbbbbb-01")}]
    body = Jason.encode!(%{jsonrpc: "2.0", method: "eth_getLogs", params: [], id: 1})

    assert {:ok, {{_, 200, _}, _, response}} =
             :httpc.request(:post, {url, headers, ~c"application/json", body}, [], [])

    assert Jason.decode!(to_string(response))["result"]
    :otel_tracer_provider.force_flush()

    records =
      for _ <- 1..3 do
        assert_receive {:span, record}, 2_000
        record
      end

    assert Enum.sort(Enum.map(records, &span(&1, :name))) == [
             "lasso.http",
             "lasso.rpc",
             "lasso.upstream"
           ]

    assert Enum.all?(records, &(span(&1, :trace_id) == String.to_integer(trace_id, 16)))
  end
end
