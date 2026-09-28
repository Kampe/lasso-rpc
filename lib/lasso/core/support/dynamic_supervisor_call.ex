defmodule Lasso.Core.Support.DynamicSupervisorCall do
  @moduledoc """
  Converts exits from DynamicSupervisor calls into typed errors.

  Dynamic supervisors may be restarting while configuration reconciliation is
  adding or removing children. Callers retain ownership of retry and logging
  policy; this module only keeps an unavailable supervisor from terminating the
  reconciliation owner.
  """

  @type supervisor :: Supervisor.supervisor()
  @type child_spec ::
          Supervisor.child_spec()
          | {module(), term()}
          | module()
          | :supervisor.child_spec()
  @type call_error :: {:supervisor_exit, term()}
  @call_timeout_ms 2_000

  @doc "Lists running children with a bound suitable for reconciliation."
  @spec children(supervisor()) :: {:ok, [tuple()]} | {:error, call_error()}
  def children(supervisor) do
    {:ok, GenServer.call(supervisor, :which_children, @call_timeout_ms)}
  catch
    :exit, reason -> {:error, {:supervisor_exit, reason}}
  end

  @spec start_child(supervisor(), child_spec()) ::
          DynamicSupervisor.on_start_child() | {:error, call_error()}
  def start_child(supervisor, child_spec) do
    bounded_call(fn -> DynamicSupervisor.start_child(supervisor, child_spec) end)
  end

  @spec terminate_child(supervisor(), pid()) :: :ok | {:error, term()}
  def terminate_child(supervisor, pid) when is_pid(pid) do
    bounded_call(fn -> DynamicSupervisor.terminate_child(supervisor, pid) end)
  end

  defp bounded_call(operation) do
    caller = self()
    token = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        result =
          try do
            operation.()
          catch
            :exit, reason -> {:error, {:supervisor_exit, reason}}
          end

        send(caller, {token, result})
      end)

    receive do
      {^token, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, reason} ->
        {:error, {:supervisor_exit, reason}}
    after
      @call_timeout_ms ->
        Process.demonitor(monitor, [:flush])
        Process.exit(pid, :kill)

        receive do
          {^token, _late_result} -> :ok
        after
          0 -> :ok
        end

        {:error, {:supervisor_exit, :timeout}}
    end
  end
end
