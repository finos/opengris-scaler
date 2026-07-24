import logging
from math import ceil
from typing import Dict, List, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot

logger = logging.getLogger(__name__)


class VanillaScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that scales workers based on task-to-worker ratio.
    """

    def __init__(self):
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        desired = self._compute_desired_worker_count(information_snapshot, worker_manager_heartbeat, managed_worker_ids)
        desired_per_capset: List[Tuple[Dict[str, int], int]] = [({}, desired)]
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _compute_desired_worker_count(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
    ) -> int:
        """Compute the target worker count for this manager from current task and worker observations."""
        current = len(managed_worker_ids)
        task_count = len(information_snapshot.tasks)
        worker_count = len(information_snapshot.workers)

        if worker_count == 0:
            desired = current + 1 if task_count > 0 else current
        else:
            task_ratio = task_count / worker_count
            if task_ratio > self._upper_task_ratio:
                desired = current + 1
            elif task_ratio < self._lower_task_ratio:
                desired = 0 if task_count == 0 else max(1, ceil(task_count / self._upper_task_ratio))
            else:
                desired = current

        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        if max_concurrency != -1:
            desired = min(desired, max_concurrency)

        desired = max(0, desired)
        if desired != current:
            logger.info(
                f"scaling {worker_manager_heartbeat.workerManagerID!r}: tasks={task_count}, "
                f"workers={worker_count}, current={current} -> desired={desired}"
            )
        return desired
