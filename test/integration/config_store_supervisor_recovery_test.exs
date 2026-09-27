defmodule Lasso.Config.ConfigStoreSupervisorRecoveryTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag capture_log: true

  alias Lasso.BlockSync.Worker
  alias Lasso.Config.{ChainConfig, ConfigStore}
  alias Lasso.Config.ChainConfig.{Monitoring, Provider, Selection, Websocket}
  alias Lasso.Providers.Catalog

  test "published profile survives BlockSync supervisor outage and restores its worker" do
    {spec, chain_id, provider_id} = profile_spec()
    supervisor = Lasso.BlockSync.DynamicSupervisor

    on_exit(fn ->
      ensure_supervisor_started(supervisor)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = Supervisor.terminate_child(Lasso.Supervisor, supervisor)
    assert Process.whereis(supervisor) == nil

    store = Process.whereis(ConfigStore)
    assert {:error, {:runtime_reconcile_pending, _}} = ConfigStore.inject_profile(spec)
    assert Process.whereis(ConfigStore) == store
    assert ConfigStore.status().runtime_reconcile_pending
    assert {:ok, _} = ConfigStore.get_chain(spec.profile_id, chain_id)

    assert {:ok, _pid} = Supervisor.restart_child(Lasso.Supervisor, supervisor)
    instance_id = Catalog.lookup_instance_id(spec.profile_id, chain_id, provider_id)
    assert is_binary(instance_id)

    assert_eventually(fn ->
      is_pid(GenServer.whereis(Worker.via(chain_id, instance_id))) and
        not ConfigStore.status().runtime_reconcile_pending
    end)
  end

  test "a restarted profile-chain supervisor restores published chains without reload" do
    {spec, chain_id, _provider_id} = profile_spec()
    supervisor = Lasso.ProfileChainSupervisor

    on_exit(fn ->
      ensure_supervisor_started(supervisor)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = ConfigStore.inject_profile(spec)
    assert Lasso.ProfileChainSupervisor.running?(spec.profile_id, chain_id)
    store = Process.whereis(ConfigStore)
    old_supervisor = Process.whereis(supervisor)
    Process.exit(old_supervisor, :kill)

    assert_eventually(fn ->
      restarted = Process.whereis(supervisor)
      is_pid(restarted) and restarted != old_supervisor
    end)

    assert_eventually(fn ->
      Lasso.ProfileChainSupervisor.running?(spec.profile_id, chain_id) and
        not ConfigStore.status().runtime_reconcile_pending
    end)

    assert Process.whereis(ConfigStore) == store
  end

  test "a stored profile update reports pending runtime work until its worker is restored" do
    {spec, chain_id, provider_id} = profile_spec()
    supervisor = Lasso.BlockSync.DynamicSupervisor

    on_exit(fn ->
      ensure_supervisor_started(supervisor)
      ConfigStore.remove_profile(spec.profile_id)
    end)

    assert :ok = ConfigStore.inject_profile(spec)
    assert :ok = Supervisor.terminate_child(Lasso.Supervisor, supervisor)

    updated_chain = %{spec.chains["runtime"] | block_time_ms: 6_000}
    updated = %{spec | chains: %{"runtime" => updated_chain}}

    assert {:error, {:runtime_reconcile_pending, _}} = ConfigStore.update_profile(updated)
    assert {:ok, stored} = ConfigStore.get_chain(spec.profile_id, chain_id)
    assert stored.block_time_ms == 6_000
    assert ConfigStore.status().runtime_reconcile_pending

    assert {:ok, _pid} = Supervisor.restart_child(Lasso.Supervisor, supervisor)
    instance_id = Catalog.lookup_instance_id(spec.profile_id, chain_id, provider_id)

    assert_eventually(fn ->
      is_pid(GenServer.whereis(Worker.via(chain_id, instance_id))) and
        not ConfigStore.status().runtime_reconcile_pending
    end)
  end

  defp profile_spec do
    suffix = System.unique_integer([:positive])
    profile_id = "runtime-reconcile-#{suffix}"
    chain_id = 8_000_000 + rem(suffix, 1_000_000)
    provider_id = "provider-#{suffix}"

    chain = %ChainConfig{
      chain_id: chain_id,
      display_name: "Runtime reconciliation",
      block_time_ms: 12_000,
      providers: [
        %Provider{
          id: provider_id,
          name: "Runtime reconciliation provider",
          url: "https://reconcile-#{suffix}.example.invalid",
          subscribe_new_heads: false
        }
      ],
      monitoring: %Monitoring{probe_interval_ms: 60_000},
      selection: %Selection{},
      websocket: %Websocket{subscribe_new_heads: false}
    }

    spec = %{
      scope: :system,
      profile_id: profile_id,
      slug: profile_id,
      name: "Runtime reconciliation",
      rps_limit: 100,
      burst_limit: 200,
      unlisted: true,
      chains: %{"runtime" => chain}
    }

    {spec, chain_id, provider_id}
  end

  defp ensure_supervisor_started(supervisor) do
    case Process.whereis(supervisor) do
      nil -> Supervisor.restart_child(Lasso.Supervisor, supervisor)
      _pid -> :ok
    end
  end

  defp assert_eventually(fun, attempts \\ 50)

  defp assert_eventually(fun, attempts) when attempts > 0 do
    if fun.() do
      assert true
    else
      Process.sleep(50)
      assert_eventually(fun, attempts - 1)
    end
  end

  defp assert_eventually(_fun, 0), do: flunk("condition not met in time")
end
