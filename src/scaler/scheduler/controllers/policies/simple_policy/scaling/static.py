from typing import Dict, List, Optional, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class StaticScalingPolicy(ScalingPolicy):
    """
    Stateless scaling policy that always asks for the same task concurrency.

    A cluster holds a fixed size through the ordinary declarative path, so it needs no separate
    fixed-mode worker manager.

    With no count, each manager is asked for the maximum task concurrency it advertises. The size
    of a fleet therefore stays with the manager that owns it, and managers of different sizes each
    get their own number without the scheduler being told about any of them.

    With a count, every manager is asked for that same number instead.
    """

    def __init__(self, task_concurrency: Optional[int] = None):
        if task_concurrency is not None and task_concurrency < 0:
            raise ValueError(f"static scaling task concurrency must not be negative, got {task_concurrency}")

        self._task_concurrency = task_concurrency

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        desired = self._compute_desired_task_concurrency(worker_manager_heartbeat)
        desired_per_capset: List[Tuple[Dict[str, int], int]] = [({}, desired)]
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _compute_desired_task_concurrency(self, worker_manager_heartbeat: WorkerManagerHeartbeat) -> int:
        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency

        if self._task_concurrency is None:
            # -1 advertises "no limit", which gives no number to hold steady.
            return max(0, max_concurrency)

        if max_concurrency != -1:
            return max(0, min(self._task_concurrency, max_concurrency))

        return self._task_concurrency
