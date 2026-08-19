from typing import Dict, List, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class VanillaScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that asks for one worker per outstanding task, bounded by the manager's
    maximum task concurrency.
    """

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
        """Compute the target worker count for this manager from the current task observation.

        A worker runs one task at a time, so the worker count is the cluster's parallelism: ask for one
        worker per outstanding task. maxTaskConcurrency is what bounds that, and asking for less than it
        while tasks are outstanding leaves capacity the operator paid for standing idle.
        """
        desired = len(information_snapshot.tasks)

        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        if max_concurrency != -1:
            desired = min(desired, max_concurrency)
        return max(0, desired)
