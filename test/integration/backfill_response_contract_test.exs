defmodule Lasso.Integration.BackfillResponseContractTest do
  use ExUnit.Case, async: false

  alias Lasso.Config.ConfigStore
  alias Lasso.Core.Support.GapFiller
  alias Lasso.JSONRPC.Quantity
  alias Lasso.Providers

  @moduletag :integration

  defmodule Upstream do
    import Plug.Conn

    def init(opts), do: opts

    def call(conn, opts) do
      {:ok, body, conn} = read_body(conn)
      request = Jason.decode!(body)

      result =
        case request["method"] do
          "eth_chainId" -> Quantity.encode(opts[:chain_id])
          "eth_blockNumber" -> opts[:head]
          "eth_getLogs" -> opts[:logs]
          _ -> "0x1"
        end

      send_resp(
        conn,
        200,
        Jason.encode!(%{"jsonrpc" => "2.0", "id" => request["id"], "result" => result})
      )
    end
  end

  test "a successful real HTTP backfill releases retained response capacity" do
    {chain, provider_id} = start_provider(head: "0x64", logs: [])
    test_pid = self()
    handler_id = {__MODULE__, make_ref()}

    :ok =
      :telemetry.attach(
        handler_id,
        [:lasso, :upstream_admission, :released],
        fn _, _, metadata, pid -> send(pid, {:capacity_released, metadata}) end,
        test_pid
      )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    plan = GapFiller.Plan.new("public", chain, provider_id, self(), 5_000)
    assert {:ok, 100} = GapFiller.fetch_head(plan)
    assert_receive {:capacity_released, %{reason: :backfill_consumed}}, 1_000
  end

  test "malformed upstream quantities return an error without crashing backfill" do
    {chain, provider_id} = start_provider(head: "0xGG", logs: [])
    plan = GapFiller.Plan.new("public", chain, provider_id, self(), 5_000)

    assert {:error, {:invalid_block_number, "0xGG"}} = GapFiller.fetch_head(plan)
  end

  test "a malformed log quantity cannot crash sorting of a real backfill response" do
    logs = [
      %{"blockNumber" => "0xGG", "logIndex" => "0x0"},
      %{"blockNumber" => "0x2", "logIndex" => "0x1"}
    ]

    {chain, provider_id} = start_provider(head: "0x2", logs: logs)
    plan = GapFiller.Plan.new("public", chain, provider_id, self(), 5_000)

    assert {:ok, sorted} = GapFiller.ensure_logs(plan, %{}, 1, 2)
    assert Enum.sort(sorted) == Enum.sort(logs)
  end

  defp start_provider(opts) do
    chain = 700_000_000 + rem(System.unique_integer([:positive]), 100_000_000)
    provider_id = "backfill-#{chain}"
    ref = {__MODULE__, chain}
    prior_http_client = Application.get_env(:lasso, :http_client)

    {:ok, _pid} =
      Plug.Cowboy.http(
        Upstream,
        [chain_id: chain, head: opts[:head], logs: opts[:logs]],
        ref: ref,
        port: 0
      )

    port = :ranch.get_port(ref)
    Application.put_env(:lasso, :http_client, Lasso.RPC.Transport.HTTP.Client.Finch)

    on_exit(fn ->
      Application.put_env(:lasso, :http_client, prior_http_client)
      Providers.remove_provider(chain, provider_id)
      Lasso.ProfileChainSupervisor.stop_profile_chain("public", chain)
      ConfigStore.unregister_chain_runtime("public", chain)
      Plug.Cowboy.shutdown(ref)
    end)

    :ok =
      ConfigStore.register_chain_runtime("public", chain, %{
        display_name: "Backfill response integration",
        providers: []
      })

    assert {:ok, ^provider_id} =
             Providers.add_provider(
               chain,
               %{id: provider_id, name: "Backfill upstream", url: "http://127.0.0.1:#{port}"},
               validate: false
             )

    {chain, provider_id}
  end
end
