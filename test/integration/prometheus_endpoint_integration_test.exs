defmodule Lasso.PrometheusEndpointIntegrationTest do
  use Lasso.Test.LassoIntegrationCase

  @moduletag :integration

  require Lasso.Test.Eventually

  alias Lasso.Observability.Prometheus

  test "a real HTTP scrape exposes bounded request and current route evidence", %{chain: chain} do
    setup_providers([%{id: "metrics_probe", profile: "public", behavior: :healthy}])

    :telemetry.execute(
      [:lasso, :rpc, :request, :stop],
      %{duration: 1},
      %{
        chain_id: chain,
        provider_id: "metrics_probe",
        method: "eth_getLogs",
        result: :error
      }
    )

    :telemetry.execute(
      [:lasso, :rpc, :request, :stop],
      %{duration: 1},
      %{
        chain_id: chain,
        provider_id: "metrics_probe",
        method: "unbounded_user_method_#{chain}",
        result: :success
      }
    )

    {:ok, _apps} = Application.ensure_all_started(:inets)
    port = LassoWeb.Endpoint.config(:http)[:port]
    url = String.to_charlist("http://127.0.0.1:#{port}/metrics")

    assert {:ok, {{_version, 200, _reason}, headers, body}} =
             :httpc.request(:get, {url, []}, [], [])

    assert {~c"content-type", content_type} =
             Enum.find(headers, fn {name, _value} -> name == ~c"content-type" end)

    assert to_string(content_type) =~ "text/plain"
    body = to_string(body)

    assert body =~
             ~s(lasso_rpc_requests_total{chain="#{chain}",provider="metrics_probe",method="eth_getLogs",outcome="error"} 1)

    assert body =~
             ~s(lasso_rpc_requests_total{chain="#{chain}",provider="metrics_probe",method="other",outcome="success"} 1)

    assert body =~
             "lasso_circuit_state{profile=\"public\",chain=\"#{chain}\",provider=\"metrics_probe\""

    assert body =~ "# TYPE lasso_provider_head_lag_blocks gauge"
    assert body =~ "# TYPE lasso_rpc_request_duration_seconds histogram"
    assert body =~ "lasso_provider_info{"
    assert body =~ "lasso_provider_head_observed{"
    assert body =~ "lasso_vm_run_queue "
    assert body =~ "lasso_observer_available 1"

    assert body =~
             ~s(lasso_rpc_request_duration_seconds_sum{profile="unknown",chain="#{chain}",provider="metrics_probe",method="eth_getLogs",transport="unknown",origin="unknown",outcome="error"} 0.001)

    refute body =~ "unbounded_user_method_#{chain}"
    assert Prometheus.stats().series <= 4_096
  end

  test "routed provider failures reach the canonical attempt counter", %{chain: chain} do
    setup_providers([
      %{id: "canonical_failure", priority: 10, behavior: :always_fail, profile: "public"}
    ])

    assert {:error, _, _} =
             RequestPipeline.execute_via_channels(
               chain,
               "eth_blockNumber",
               [],
               %RequestOptions{
                 profile: "public",
                 provider_override: "canonical_failure",
                 failover_on_override: false,
                 strategy: :priority,
                 timeout_ms: 1000,
                 request_id: "metrics-canonical-failure"
               }
             )

    Lasso.Test.Eventually.assert_eventually(fn ->
      Prometheus.scrape() =~
        ~s(lasso_upstream_attempts_total{chain="#{chain}",provider="canonical_failure",transport="http")
    end)
  end

  test "multi-route scrape groups declarations before contiguous family samples", %{chain: chain} do
    setup_providers([
      %{id: "format_a", profile: "public", behavior: :healthy},
      %{id: "format_b", profile: "public", behavior: :healthy}
    ])

    body = Prometheus.scrape()
    assert body =~ ~s(provider="format_a")
    assert body =~ ~s(provider="format_b")
    lines = String.split(body, "\n", trim: true)
    names = for "# TYPE " <> rest <- lines, do: rest |> String.split(" ") |> hd()
    assert length(names) == length(Enum.uniq(names))
    families = MapSet.new(names)

    sequence =
      Enum.map(lines, fn line ->
        case line do
          "# HELP " <> rest ->
            rest |> String.split(" ") |> hd()

          "# TYPE " <> rest ->
            rest |> String.split(" ") |> hd()

          sample ->
            name = sample |> String.split(["{", " "], parts: 2) |> hd()

            if MapSet.member?(families, name),
              do: name,
              else: String.replace(name, ~r/_(bucket|sum|count)$/, "")
        end
      end)

    groups = sequence |> Enum.chunk_by(& &1) |> Enum.map(&hd/1)
    assert length(groups) == length(Enum.uniq(groups))

    for name <- names do
      type_index = Enum.find_index(lines, &String.starts_with?(&1, "# TYPE #{name} "))

      samples =
        Enum.with_index(lines)
        |> Enum.filter(fn {line, _} ->
          not String.starts_with?(line, "#") and String.starts_with?(line, name)
        end)

      for {_line, index} <- samples, do: assert(type_index < index)
    end

    assert body =~ ~s(chain="#{chain}")
  end
end
