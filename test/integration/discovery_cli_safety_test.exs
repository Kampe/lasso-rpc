defmodule Lasso.Integration.DiscoveryCliSafetyTest do
  use ExUnit.Case, async: false

  @moduletag :integration

  alias Lasso.Discovery
  alias Lasso.Discovery.{Formatter, Probes.Limits}

  defmodule Upstream do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, opts) do
      {:ok, body, conn} = read_body(conn)
      request = Jason.decode!(body)
      method = request["method"]
      send(opts[:observer], {:discovery_request, method, request["params"]})

      response =
        case {opts[:mode], method} do
          {:identity_unavailable, "eth_chainId"} ->
            error(request, -32_000, "chain identity unavailable")

          {:wrong_block_id, "eth_blockNumber"} ->
            %{jsonrpc: "2.0", id: 999, result: "0x200000"}

          {:invalid_height, "eth_blockNumber"} ->
            success(request, "0xnot-a-quantity")

          {:young_chain, "eth_blockNumber"} ->
            success(request, "0x5")

          {:range_error, "eth_getLogs"} ->
            error(request, -32_601, "Method not found")

          {_, "eth_chainId"} ->
            success(request, "0x89")

          {_, "eth_blockNumber"} ->
            success(request, "0x200000")

          {_, "eth_getBalance"} ->
            success(request, "0x0")

          {_, "eth_getLogs"} ->
            success(request, [])

          {_, "eth_getBlockByNumber"} ->
            success(request, %{"number" => "0x1"})

          _ ->
            error(request, -32_601, "Method not found")
        end

      send_resp(conn, 200, Jason.encode!(response))
    end

    defp success(request, result),
      do: %{jsonrpc: "2.0", id: request["id"], result: result}

    defp error(request, code, message),
      do: %{jsonrpc: "2.0", id: request["id"], error: %{code: code, message: message}}
  end

  test "discovery reports an endpoint without its URL credentials" do
    url = "https://operator:password@example.com/private-token?api_key=secret"
    results = Discovery.probe(url, probes: [])

    for report <- [Formatter.format_table(results), Formatter.format_json(results)] do
      assert report =~ "example.com"
      refute report =~ "operator"
      refute report =~ "password"
      refute report =~ "private-token"
      refute report =~ "secret"
    end

    failed = Map.put(results, :websocket, %{connected: false, error: "failed at #{url}"})

    for report <- [Formatter.format_table(failed), Formatter.format_json(failed)] do
      assert report =~ "example.com"
      refute report =~ "password"
      refute report =~ "private-token"
      refute report =~ "secret"
    end

    assert %{"url" => "https://example.com"} =
             failed |> Formatter.format_json() |> Jason.decode!()
  end

  test "limit discovery stops before depth probes when chain identity is unavailable" do
    url = start_upstream(:identity_unavailable)

    assert %{identity: %{status: :error}} =
             Discovery.probe_limits(url, tests: [:archive_support], timeout: 1_000)

    assert_receive {:discovery_request, "eth_chainId", []}
    refute_receive {:discovery_request, "eth_blockNumber", _}
    refute_receive {:discovery_request, "eth_getBlockByNumber", _}
    refute_receive {:discovery_request, "eth_getBalance", _}
  end

  test "archive probe rejects wrong-ID height evidence and does not infer retention from empty logs" do
    wrong_id_url = start_upstream(:wrong_block_id)

    assert %{archive_support: %{status: :inconclusive}} =
             Limits.probe(wrong_id_url, tests: [:archive_support], timeout: 1_000)

    invalid_height_url = start_upstream(:invalid_height)

    assert %{archive_support: %{status: :inconclusive}} =
             Limits.probe(invalid_height_url, tests: [:archive_support], timeout: 1_000)

    retained_state_url = start_upstream(:retained_state)

    assert %{archive_support: %{status: :supported, value: :archive_state_only}} =
             Limits.probe(retained_state_url, tests: [:archive_support], timeout: 1_000)
  end

  test "range probing needs observed success and a valid range on a young chain" do
    unavailable_logs_url = start_upstream(:range_error)

    assert %{block_range: %{status: :inconclusive}} =
             Limits.probe(unavailable_logs_url, tests: [:block_range], timeout: 1_000)

    young_chain_url = start_upstream(:young_chain)

    assert %{block_range: %{status: :inconclusive}} =
             Limits.probe(young_chain_url, tests: [:block_range], timeout: 1_000)

    assert %{archive_support: %{status: :inconclusive}} =
             Limits.probe(young_chain_url, tests: [:archive_support], timeout: 1_000)
  end

  defp start_upstream(mode) do
    ref = {__MODULE__, make_ref()}
    previous = Application.get_env(:lasso, :http_client)

    {:ok, _pid} = Plug.Cowboy.http(Upstream, [observer: self(), mode: mode], ref: ref, port: 0)
    Application.put_env(:lasso, :http_client, Lasso.RPC.Transport.HTTP.Client.Finch)

    on_exit(fn ->
      Application.put_env(:lasso, :http_client, previous)
      Plug.Cowboy.shutdown(ref)
    end)

    "http://127.0.0.1:#{:ranch.get_port(ref)}"
  end
end
