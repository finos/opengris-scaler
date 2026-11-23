import logging
from math import ceil
from typing import Dict, List, Tuple

from scaler.config.constants import VANILLA_LOWER_TASK_RATIO, VANILLA_UPPER_TASK_RATIO
from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.types import WorkerManagerBounds
from scaler.scheduler.policies.scaling.utility import build_set_desired_command, forget_departed_managers
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerManagerID

logger = logging.getLogger(__name__)


class VanillaScalingPolicy(ScalingPolicy):
    """
    Scaling policy that scales workers based on task-to-worker ratio.

    Decisions depend only on the current observation. The last count logged per manager is kept for logging.
    Each manager's count is kept within its bounds; a manager without bounds ranges from 0 to its advertised maximum.
    """

    def __init__(self, bounds: Dict[WorkerManagerID, WorkerManagerBounds]):
        self._bounds = bounds
        self._logged_desired_by_manager: Dict[WorkerManagerID, int] = {}

    def get_scaling_commands(
        self, worker_manager_id: WorkerManagerID, cluster_state: ClusterState
    ) -> List[WorkerManagerCommand]:
        forget_departed_managers(self._logged_desired_by_manager, set(cluster_state.managers))

        desired = self._compute_desired_worker_count(worker_manager_id, cluster_state)
        desired_per_capset: List[Tuple[Dict[str, int], int]] = [({}, desired)]
        return [build_set_desired_command(desired_per_capset)]

    def _compute_desired_worker_count(self, manager_id: WorkerManagerID, cluster_state: ClusterState) -> int:
        """Compute the target worker count for this manager from the cluster state."""
        info = cluster_state.managers[manager_id]
        current = info.worker_count
        task_count = cluster_state.n_tasks
        serving_workers = cluster_state.n_workers - cluster_state.n_draining_workers

        floor_held = floor_held_worker_count(manager_id, self._bounds, cluster_state.managers)
        desired = ratio_target(current, task_count, serving_workers, floor_held)
        bounds = self._bounds.get(manager_id, WorkerManagerBounds(max_task_concurrency=None))
        advertised_max = info.max_task_concurrency or -1
        desired = clamp_to_bounds(desired, bounds, advertised_max)
        self.__log_decision(manager_id, task_count, serving_workers, current, desired)
        return desired

    def __log_decision(
        self, manager_id: WorkerManagerID, task_count: int, worker_count: int, current: int, desired: int
    ) -> None:
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


def ratio_target(current: int, task_count: int, worker_count: int, floor_held: int) -> int:
    """The count the task-to-worker ratio asks of a manager that holds `current` workers, before its bounds.

    Shrinking aims at one worker per VANILLA_UPPER_TASK_RATIO tasks, less the `floor_held` other managers keep."""
    if worker_count == 0:
        return current + 1 if task_count > 0 else current

    task_ratio = task_count / worker_count
    if task_ratio > VANILLA_UPPER_TASK_RATIO:
        return current + 1
    if task_ratio < VANILLA_LOWER_TASK_RATIO:
        return max(0, ceil(task_count / VANILLA_UPPER_TASK_RATIO) - floor_held)
    return current


def clamp_to_bounds(desired: int, bounds: WorkerManagerBounds, advertised_max_task_concurrency: int) -> int:
    """Keep `desired` within `bounds`, whose cap never exceeds the manager's advertised maximum (-1: none)."""
    caps = [cap for cap in (bounds.max_task_concurrency, advertised_max_task_concurrency) if cap not in (None, -1)]
    if not caps:
        return max(bounds.min_task_concurrency, desired)

    cap = min(caps)
    return max(min(bounds.min_task_concurrency, cap), min(desired, cap))


def floor_held_worker_count(
    manager_id: WorkerManagerID,
    bounds_by_manager: Dict[WorkerManagerID, WorkerManagerBounds],
    worker_manager_infos: Dict[WorkerManagerID, WorkerManagerInfo],
) -> int:
    """The connected workers that managers other than `manager_id` hold at their floors.

    A manager never drops below its floor, so these workers are safe to count as supply. Workers above a floor
    are not: two scaling managers sized from one snapshot would each subtract the other and drain together."""
    return sum(
        min(info.worker_count, bounds_by_manager[other_id].min_task_concurrency)
        for other_id, info in worker_manager_infos.items()
        if other_id != manager_id and other_id in bounds_by_manager
    )
