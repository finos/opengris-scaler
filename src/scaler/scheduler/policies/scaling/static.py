from typing import Dict, List, Optional

from scaler.protocol.capnp import WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.policies.scaling.utility import build_set_desired_command
from scaler.utility.identifiers import WorkerID, WorkerManagerID
from scaler.utility.snapshot import InformationSnapshot


class StaticScalingPolicy(ScalingPolicy):
    """Requests a fixed task concurrency from each worker manager, whatever the load.

    A manager named in `task_concurrency_by_manager` gets its own count, any other gets `task_concurrency`.
    With no count, a manager is asked for the maximum task concurrency it advertises, which also caps every count.
    """

    def __init__(self, task_concurrency: Optional[int], task_concurrency_by_manager: Dict[WorkerManagerID, int]):
        self._task_concurrency = task_concurrency
        self._task_concurrency_by_manager = task_concurrency_by_manager

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[WorkerManagerID, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        desired = worker_manager_heartbeat.maxTaskConcurrency
        task_concurrency = self._task_concurrency_by_manager.get(
            worker_manager_heartbeat.workerManagerID, self._task_concurrency
        )
        if task_concurrency is not None:
            desired = min(task_concurrency, desired)
        return [build_set_desired_command([({}, desired)])]
