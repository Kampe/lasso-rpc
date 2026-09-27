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

  @spec start_child(supervisor(), child_spec()) ::
          DynamicSupervisor.on_start_child() | {:error, call_error()}
  def start_child(supervisor, child_spec) do
    DynamicSupervisor.start_child(supervisor, child_spec)
  catch
    :exit, reason -> {:error, {:supervisor_exit, reason}}
  end

  @spec terminate_child(supervisor(), pid()) :: :ok | {:error, term()}
  def terminate_child(supervisor, pid) when is_pid(pid) do
    DynamicSupervisor.terminate_child(supervisor, pid)
  catch
    :exit, reason -> {:error, {:supervisor_exit, reason}}
  end
end
