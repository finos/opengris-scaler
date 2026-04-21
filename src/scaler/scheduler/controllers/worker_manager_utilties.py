from typing import Dict, List, Optional

from scaler.protocol import capnp
from scaler.protocol.capnp import ScalingManagerStatus


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
