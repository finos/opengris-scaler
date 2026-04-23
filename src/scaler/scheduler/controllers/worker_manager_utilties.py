from typing import Dict, List, Optional, Tuple

from scaler.protocol import capnp
from scaler.protocol.capnp import ScalingManagerStatus, TaskCapability, WorkerManagerCommand, WorkerManagerCommandType


def build_scaling_manager_status(
    managed_workers: Dict[bytes, list], worker_manager_details: Optional[List[dict]] = None
) -> ScalingManagerStatus:
    details = worker_manager_details or []
    return capnp.ScalingManagerStatus(
        managedWorkers=[
            capnp.ScalingManagerStatus.Pair(
                workerManagerID=worker_manager_id, workerIDs=[bytes(worker_id) for worker_id in worker_ids]
            )
            for worker_manager_id, worker_ids in managed_workers.items()
        ],
        workerManagerDetails=[
            capnp.ScalingManagerStatus.WorkerManagerDetail(
                workerManagerID=d["worker_manager_id"],
                identity=d["identity"],
                lastSeenS=d["last_seen_s"],
                maxTaskConcurrency=d["max_task_concurrency"],
                capabilities=d.get("capabilities", ""),
                pendingWorkers=d.get("pending_workers", 0),
            )
            for d in details
        ],
    )


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
    return WorkerManagerCommand(
        workerIDs=[],
        command=WorkerManagerCommandType.setDesiredTaskConcurrency,
        capabilities=[],
        setDesiredTaskConcurrencyRequests=requests,
    )
