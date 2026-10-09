import logging
from math import ceil
from typing import Dict, List, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerBounds, WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import (
    build_scaling_manager_status,
    build_set_desired_command,
    forget_departed_managers,
)
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot

logger = logging.getLogger(__name__)


class VanillaScalingPolicy(ScalingPolicy):
    """
    Scaling policy that scales workers based on task-to-worker ratio.

    Decisions depend only on the current observation. The last count logged per manager is kept for logging.
    Each manager's count is kept within its bounds; a manager without bounds ranges from 0 to its advertised maximum.
    """

    def __init__(self, bounds: Dict[bytes, WorkerManagerBounds]):
        self._bounds = bounds
        self._lower_task_ratio = 1
        self._upper_task_ratio = 10
        self._logged_desired_by_manager: Dict[bytes, int] = {}

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        # the manager being answered counts as live even if no snapshot was built for it
        live_manager_ids = set(worker_manager_snapshots) | {worker_manager_heartbeat.workerManagerID}
        forget_departed_managers(self._logged_desired_by_manager, live_manager_ids)

        desired = self._compute_desired_worker_count(
            information_snapshot, worker_manager_heartbeat, managed_worker_ids, worker_manager_snapshots
        )
        desired_per_capset: List[Tuple[Dict[str, int], int]] = [({}, desired)]
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

    def _compute_desired_worker_count(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> int:
        """Compute the target worker count for this manager from current task and worker observations."""
        manager_id = worker_manager_heartbeat.workerManagerID
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
                needed = 0 if task_count == 0 else max(1, ceil(task_count / self._upper_task_ratio))
                desired = max(0, needed - self._floor_held_worker_count(manager_id, worker_manager_snapshots))
            else:
                desired = current

        desired = self._clamp_to_bounds(manager_id, worker_manager_heartbeat.maxTaskConcurrency, desired)
        self.__log_decision(manager_id, task_count, worker_count, current, desired)
        return desired

    def _clamp_to_bounds(self, manager_id: bytes, advertised_max_task_concurrency: int, desired: int) -> int:
        """Keep `desired` within the manager's bounds, whose cap never exceeds the manager's advertised maximum."""
        bounds = self._bounds.get(manager_id, WorkerManagerBounds(max_task_concurrency=None))
        caps = [cap for cap in (bounds.max_task_concurrency, advertised_max_task_concurrency) if cap not in (None, -1)]
        if not caps:
            return max(bounds.min_task_concurrency, desired)

        cap = min(caps)
        return max(min(bounds.min_task_concurrency, cap), min(desired, cap))

    def _floor_held_worker_count(
        self, manager_id: bytes, worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot]
    ) -> int:
        """The connected workers that other managers hold at their floors.

        A manager never drops below its floor, so these workers are safe to count as supply. Workers above a floor
        are not: two scaling managers sized from one snapshot would each subtract the other and drain together."""
        return sum(
            min(snapshot.worker_count, self._bounds[other_id].min_task_concurrency)
            for other_id, snapshot in worker_manager_snapshots.items()
            if other_id != manager_id and other_id in self._bounds
        )

    def __log_decision(self, manager_id: bytes, task_count: int, worker_count: int, current: int, desired: int) -> None:
        """Logs a request to change a manager's worker count, once per run of that same request.

        The request repeats every heartbeat until the manager reaches the count, and forever if it cannot.
        """

        if desired == current:
            self._logged_desired_by_manager.pop(manager_id, None)
            return

        if self._logged_desired_by_manager.get(manager_id) == desired:
            return

        self._logged_desired_by_manager[manager_id] = desired
        logger.info(
            f"scaling {manager_id!r}: tasks={task_count}, workers={worker_count}, current={current} -> "
            f"desired={desired}"
        )
