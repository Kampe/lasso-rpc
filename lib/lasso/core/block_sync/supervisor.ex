defmodule Lasso.BlockSync.Supervisor do
  @moduledoc """
  Singleton interface to the BlockSync DynamicSupervisor.

  Manages one Worker per (chain_id, instance_id) pair. Workers are started
  during `start_shared_infrastructure` and can be added/removed at runtime.
  """

  require Logger

  alias Lasso.BlockSync.Worker
  alias Lasso.Core.Support.DynamicSupervisorCall

  @dynamic_supervisor Lasso.BlockSync.DynamicSupervisor

  @spec start_worker(pos_integer(), String.t()) :: {:ok, pid()} | {:error, term()}
  def start_worker(chain_id, instance_id)
      when is_integer(chain_id) and chain_id > 0 and is_binary(instance_id) do
    spec = {Worker, {chain_id, instance_id}}

    case DynamicSupervisorCall.start_child(@dynamic_supervisor, spec) do
      {:ok, pid} ->
        {:ok, pid}

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, {:supervisor_exit, _reason}} = error ->
        error

      {:error, reason} ->
        Logger.warning("Failed to start BlockSync worker",
          chain_id: chain_id,
          instance_id: instance_id,
          reason: inspect(reason)
        )

        {:error, reason}
    end
  end

  @spec stop_worker(pos_integer(), String.t()) :: :ok | {:error, term()}
  def stop_worker(chain_id, instance_id)
      when is_integer(chain_id) and chain_id > 0 and is_binary(instance_id) do
    case GenServer.whereis(Worker.via(chain_id, instance_id)) do
      nil ->
        :ok

      pid ->
        case DynamicSupervisorCall.terminate_child(@dynamic_supervisor, pid) do
          :ok -> :ok
          {:error, _} = error -> error
        end
    end
  end

  @spec start_all_workers(pos_integer(), [String.t()]) :: :ok
  def start_all_workers(chain_id, instance_ids)
      when is_integer(chain_id) and chain_id > 0 and is_list(instance_ids) do
    Logger.info("Starting BlockSync workers",
      chain_id: chain_id,
      instance_count: length(instance_ids)
    )

    Enum.each(instance_ids, fn instance_id ->
      start_worker(chain_id, instance_id)
    end)
  end

  @spec list_workers() :: [pid()]
  def list_workers do
    DynamicSupervisor.which_children(@dynamic_supervisor)
    |> Enum.map(fn {_, pid, _, _} -> pid end)
    |> Enum.reject(&is_nil/1)
  end

  @doc "Returns running worker identities independently of the current catalog snapshot."
  @spec list_worker_keys() :: {:ok, [{pos_integer(), String.t()}]} | {:error, term()}
  def list_worker_keys do
    @dynamic_supervisor
    |> GenServer.call(:which_children, 2_000)
    |> Enum.flat_map(fn
      {_, pid, _, _} when is_pid(pid) ->
        Lasso.Registry
        |> Registry.keys(pid)
        |> Enum.flat_map(fn
          {:block_sync_worker, chain_id, instance_id} -> [{chain_id, instance_id}]
          _ -> []
        end)

      _ ->
        []
    end)
    |> then(&{:ok, &1})
  catch
    :exit, reason -> {:error, {:supervisor_exit, reason}}
  end
end
