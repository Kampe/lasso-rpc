defmodule Lasso.Integration.DiscoveryWebSocketProbeTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Lasso.Discovery.Probes.WebSocket

  setup do
    prior = Application.get_env(:lasso, :custom_origin_resolver)
    Application.put_env(:lasso, :custom_origin_resolver, fn _ -> {:ok, [{127, 0, 0, 1}]} end)

    on_exit(fn ->
      if prior,
        do: Application.put_env(:lasso, :custom_origin_resolver, prior),
        else: Application.delete_env(:lasso, :custom_origin_resolver)
    end)

    :ok
  end

  defmodule Endpoint do
    @behaviour :cowboy_websocket
    @hash "0x" <> String.duplicate("1", 64)

    def init(req, state) do
      send(state.parent, :ws_connected)
      {:cowboy_websocket, req, state}
    end

    def websocket_init(state), do: {:ok, state}

    def websocket_handle({:text, body}, state) do
      request = Jason.decode!(body)
      send(state.parent, {:ws_request, request["method"]})

      case request do
        %{"method" => method}
        when method in ["eth_subscribe", "eth_gasPrice"] and not is_nil(state.error) ->
          frame =
            {:text,
             Jason.encode!(%{"jsonrpc" => "2.0", "id" => request["id"], "error" => state.error})}

          {:reply, frame, state}

        %{"method" => "eth_subscribe", "params" => [type | _]} ->
          reply = result(request, type)

          frames =
            if state.events do
              event =
                case {state.events, type} do
                  {:malformed, "newHeads"} ->
                    %{"number" => "0x1"}

                  {:malformed, "logs"} ->
                    %{"topics" => []}

                  {:malformed, "newPendingTransactions"} ->
                    "0x1"

                  {_, "newHeads"} ->
                    %{"number" => "0x1", "hash" => @hash}

                  {_, "logs"} ->
                    %{
                      "address" => "0x" <> String.duplicate("1", 40),
                      "data" => "0x",
                      "topics" => [],
                      "blockNumber" => "0x1"
                    }

                  {_, "newPendingTransactions"} ->
                    @hash
                end

              [
                reply,
                {:text,
                 Jason.encode!(%{
                   "jsonrpc" => "2.0",
                   "method" => "eth_subscription",
                   "params" => %{"subscription" => type, "result" => event}
                 })}
              ]
            else
              [reply]
            end

          {:reply, frames, state}

        %{"method" => "eth_chainId"} ->
          {:reply, result(request, state.chain_id), state}

        %{"method" => "eth_unsubscribe"} ->
          {:reply, result(request, true), state}

        _ ->
          {:reply, result(request, "0x1"), state}
      end
    end

    def websocket_handle(_, state), do: {:ok, state}
    def websocket_info(_, state), do: {:ok, state}

    defp result(request, value),
      do: {:text, Jason.encode!(%{"jsonrpc" => "2.0", "id" => request["id"], "result" => value})}
  end

  defp endpoint(opts) do
    ref = make_ref()

    state = %{
      parent: self(),
      chain_id: Keyword.get(opts, :chain_id, "0x2105"),
      events: Keyword.get(opts, :events, true),
      error: Keyword.get(opts, :error)
    }

    dispatch = :cowboy_router.compile([{:_, [{"/ws", Endpoint, state}]}])
    {:ok, _} = :cowboy.start_clear(ref, %{socket_opts: [port: 0]}, %{env: %{dispatch: dispatch}})
    on_exit(fn -> :cowboy.stop_listener(ref) end)
    "ws://127.0.0.1:#{:ranch.get_port(ref)}/ws"
  end

  test "a different chain cannot supply generated WebSocket capabilities" do
    result = WebSocket.probe(endpoint(chain_id: "0x1"), chain_id: 8453, timeout: 500)
    refute result.connected
    assert result.error == "chain_mismatch"
    refute_received {:ws_request, "eth_subscribe"}
  end

  test "valid unary results and subscription events verify the endpoint" do
    result = WebSocket.probe(endpoint([]), chain_id: 8453, timeout: 500)
    assert result.connected
    assert result.unary_requests["eth_chainId"].status == :supported

    for topic <- ["newHeads", "logs", "newPendingTransactions"] do
      assert result.subscriptions[topic].status == :supported
      assert result.subscriptions[topic].received_event
    end
  end

  test "a quiet observation window establishes acceptance only" do
    result =
      WebSocket.probe(endpoint(events: false),
        chain_id: 8453,
        timeout: 500,
        subscription_wait: 100
      )

    assert result.connected

    for topic <- ["newHeads", "logs", "newPendingTransactions"] do
      assert result.subscriptions[topic].status == :accepted
      refute result.subscriptions[topic].received_event
    end

    report =
      Lasso.Discovery.Formatter.format_table(%{
        url: "wss://example.com",
        probes_run: [:websocket],
        timestamp: DateTime.utc_now(),
        websocket: result
      })

    assert report =~ "? newHeads: accepted"
  end

  test "malformed subscription events cannot verify capability" do
    result =
      WebSocket.probe(endpoint(events: :malformed),
        chain_id: 8453,
        timeout: 500,
        subscription_wait: 100
      )

    assert result.connected

    for topic <- ["newHeads", "logs", "newPendingTransactions"] do
      assert result.subscriptions[topic].status == :accepted
      refute result.subscriptions[topic].received_event
    end
  end

  test "WebSocket probing applies custom policy to unary and subscription errors" do
    error = %{"code" => 23, "message" => "Unsupported subscription: newHeads for this API key"}
    url = endpoint(error: error)

    caps = %{
      error_rules: [
        %{code: 23, message_contains: "unsupported subscription", category: :rate_limit}
      ]
    }

    result = WebSocket.probe(url, chain_id: 8453, timeout: 500, provider_capabilities: caps)
    assert result.unary_requests["eth_gasPrice"].status == :unknown
    assert result.unary_requests["eth_gasPrice"].error_category == :rate_limit
    assert result.subscriptions["newHeads"].status == :unknown
    assert result.subscriptions["newHeads"].error_category == :rate_limit

    result = WebSocket.probe(url, chain_id: 8453, timeout: 500)
    assert result.subscriptions["newHeads"].status == :unsupported
    assert result.subscriptions["newHeads"].error_category == :capability_violation
  end
end
