from typing import Dict, Iterable, List, Tuple, TypeVar

from scaler.protocol.capnp import TaskCapability, WorkerManagerCommand
from scaler.utility.identifiers import WorkerManagerID

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
