from math import ceil
from typing import Dict, List, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import (
    build_scaling_manager_status,
    build_set_desired_command,
    effective_desired_for_manager,
)
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


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
        effective = effective_desired_for_manager(desired_per_capset, worker_manager_heartbeat.capabilities)
        if effective == len(managed_worker_ids):
            return []
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
                if task_count > 0:
                    # Native worker managers only receive a target concurrency,
                    # not which concrete worker is safe to stop. If we scale
                    # down while tasks are still in flight, the provisioner may
                    # tear down an arbitrary active worker and force task
                    # failure/retry churn mid-computation. Keep the current
                    # pool size until the scheduler-visible task set is empty,
                    # then let worker_timeout_seconds reap truly idle workers.
                    desired = current
                else:
                    desired = current
            else:
                desired = current

        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        if max_concurrency != -1:
            desired = min(desired, max_concurrency)
        return max(0, desired)
