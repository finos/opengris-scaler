from typing import Dict, FrozenSet, Iterable, List, Tuple, TypeVar

from scaler.protocol.capnp import TaskCapability, WorkerManagerCommand
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.snapshot import InformationSnapshot

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


def build_set_desired_command(desired_per_capset: List[Tuple[Dict[str, int], int]]) -> WorkerManagerCommand:
    """Build a declarative setDesiredTaskConcurrency command.

    Each entry in desired_per_capset maps a capability set (as Dict[str, int]) to a
    desired worker count. An empty list is valid and yields a command whose requests
    list is empty (declarative "no opinion").
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
    information_snapshot: InformationSnapshot, worker_manager_infos: Dict[WorkerManagerID, WorkerManagerInfo]
) -> ClusterState:
    """Build an immutable ClusterState from live observations.

    Each capset's representative is the first task's capabilities dict.
    """

    n_tasks_by_capset: Dict[FrozenSet[str], int] = {}
    capset_representatives: Dict[FrozenSet[str], Dict[str, int]] = {}
    for task in information_snapshot.tasks.values():
        capset = frozenset(task.capabilities.keys())
        n_tasks_by_capset[capset] = n_tasks_by_capset.get(capset, 0) + 1
        if capset not in capset_representatives:
            capset_representatives[capset] = task.capabilities

    return ClusterState(
        n_tasks=len(information_snapshot.tasks),
        n_workers=len(information_snapshot.workers),
        n_tasks_by_capset=n_tasks_by_capset,
        capset_representatives=capset_representatives,
        managers=worker_manager_infos,
    )
