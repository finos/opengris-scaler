from types import MappingProxyType
from typing import Any, Dict, FrozenSet, Iterable, Mapping, Sequence, Tuple, TypeVar

from scaler.protocol.capnp import TaskCapability, WorkerManagerCommand
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import TaskID, WorkerID, WorkerManagerID

StateT = TypeVar("StateT")


def forget_departed_managers(
    state_by_manager: Dict[WorkerManagerID, StateT], live_manager_ids: Iterable[WorkerManagerID]
) -> None:
    """Drops per-manager state for managers the scheduler no longer knows about.

    A policy is only ever told about a manager that is heartbeating, so anything it remembers per manager id
    otherwise stays for the lifetime of the scheduler -- growing without bound wherever ids are not reused
    across restarts, e.g. one derived from a pod or instance name.
    """

    live_ids = set(live_manager_ids)
    for departed_id in [manager_id for manager_id in state_by_manager if manager_id not in live_ids]:
        del state_by_manager[departed_id]


def build_set_desired_command(desired_per_capset: Sequence[Tuple[Mapping[str, int], int]]) -> WorkerManagerCommand:
    """Build a declarative setDesiredTaskConcurrency command.

    Each entry in desired_per_capset pairs a capability mapping with a desired worker count. An empty list is valid and
    yields a command whose requests list is empty (declarative "no opinion").
    """
    requests = [
        WorkerManagerCommand.DesiredTaskConcurrencyRequest(
            taskConcurrency=max(0, count),
            capabilities=[TaskCapability(name=name, value=value) for name, value in caps.items()],
        )
        for caps, count in desired_per_capset
    ]
    return WorkerManagerCommand(setDesiredTaskConcurrencyRequests=requests)


def build_cluster_state(
    tasks: Mapping[TaskID, Any],
    workers: Mapping[WorkerID, Any],
    worker_manager_infos: Mapping[WorkerManagerID, WorkerManagerInfo],
) -> ClusterState:
    """Build an immutable ClusterState from live observations.

    Each capset's representative views the first task's capabilities dictionary.
    """

    n_tasks_by_capset: Dict[FrozenSet[str], int] = {}
    capset_representatives: Dict[FrozenSet[str], Mapping[str, int]] = {}
    for task in tasks.values():
        capset = frozenset(task.capabilities.keys())
        n_tasks_by_capset[capset] = n_tasks_by_capset.get(capset, 0) + 1
        if capset not in capset_representatives:
            capset_representatives[capset] = MappingProxyType(task.capabilities)

    return ClusterState(
        n_tasks=len(tasks),
        n_workers=len(workers),
        n_draining_workers=sum(1 for w in workers.values() if w.draining),
        n_tasks_by_capset=MappingProxyType(n_tasks_by_capset),
        capset_representatives=MappingProxyType(capset_representatives),
        managers=MappingProxyType(worker_manager_infos),
    )
