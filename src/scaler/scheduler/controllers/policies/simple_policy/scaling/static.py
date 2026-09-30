from typing import Dict, List, Optional

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class StaticScalingPolicy(ScalingPolicy):
    """Requests the same task concurrency from every worker manager, whatever the load.

    With no count, each manager is asked for the maximum task concurrency it advertises.
    """

    def __init__(self, task_concurrency: Optional[int]):
        self._task_concurrency = task_concurrency

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        desired = worker_manager_heartbeat.maxTaskConcurrency
        if self._task_concurrency is not None:
            desired = min(self._task_concurrency, desired)
        return [build_set_desired_command([({}, desired)])]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)
