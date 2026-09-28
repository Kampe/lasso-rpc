defmodule Lasso.Integration.RequestDeadlineExhaustionTest do
  use Lasso.Test.LassoIntegrationCase

  alias Lasso.JSONRPC.Error, as: JError
  alias Lasso.RPC.{RequestOptions, RequestPipeline, Response}
  alias Lasso.Testing.MockProviderBehavior

  test "one replay-safe provider receives the whole request deadline", %{chain: chain} do
    setup_providers([
      %{
        id: "only-slow-reader",
        priority: 1,
        background_observations: false,
        behavior:
          {:conditional,
           fn method, params, state ->
             Process.sleep(500)
             MockProviderBehavior.execute_behavior(:healthy, method, params, state)
           end}
      }
    ])

    assert {:ok, response, ctx} =
             RequestPipeline.execute_via_channels(chain, "eth_blockNumber", [], options(700))

    assert ctx.executed_channel.provider_id == "only-slow-reader"
    assert ctx.execution_envelope.dispatch_count == 1
    assert {:ok, _height} = Response.Success.decode_result(response)
  end

  test "extended reads retain a bounded fallback dispatch", %{chain: chain} do
    setup_providers([
      %{id: "first-reader", priority: 1, behavior: :always_fail},
      %{id: "second-reader", priority: 2, behavior: :healthy}
    ])

    assert {:ok, response, ctx} =
             RequestPipeline.execute_via_channels(
               chain,
               "eth_getBlockReceipts",
               ["0x1"],
               options(1_000)
             )

    assert ctx.executed_channel.provider_id == "second-reader"
    assert ctx.execution_envelope.dispatch_count == 2
    assert {:ok, _result} = Response.Success.decode_result(response)
  end

  test "dispatch-budget exhaustion preserves the final upstream error", %{chain: chain} do
    upstream =
      JError.new(-32_077, "provider refused this read",
        category: :server_error,
        retriable?: true
      )

    setup_providers([
      %{id: "first-refusal", priority: 1, behavior: :always_fail},
      %{id: "second-refusal", priority: 2, behavior: :always_fail},
      %{id: "final-refusal", priority: 3, behavior: {:error, upstream}},
      %{id: "beyond-budget", priority: 4, behavior: :healthy}
    ])

    assert {:error, error, ctx} =
             RequestPipeline.execute_via_channels(chain, "eth_blockNumber", [], options(1_000))

    assert error.code == -32_077
    assert error.message == "provider refused this read"
    assert ctx.execution_envelope.dispatch_count == 3
  end

  defp options(timeout_ms) do
    %RequestOptions{
      profile: "public",
      strategy: :priority,
      transport: :http,
      timeout_ms: timeout_ms
    }
  end
end
