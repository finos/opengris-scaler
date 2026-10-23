from math import ceil
from typing import Dict, List, Optional, Tuple

from scaler.scheduler.policies.scaling.estimator import ClusterState, ManagerInfo, ScalingEstimator


class IncrementalRatioEstimator(ScalingEstimator):
    """Ratio-based estimator that adjusts worker count incrementally.

    Scales up by one when the task/worker ratio exceeds the upper threshold, scales down to a floor when below the
    lower threshold, and holds steady in between.

    Raises NotImplementedError if the cluster state contains capability-aware tasks.
    """

    def __init__(self, lower_task_ratio: int = 1, upper_task_ratio: int = 10):
        self._lower_task_ratio = lower_task_ratio
        self._upper_task_ratio = upper_task_ratio
        self._cluster_state: Optional[ClusterState] = None

    def update(self, cluster_state: ClusterState) -> None:
        self._cluster_state = cluster_state

    def estimate(self, manager_id: bytes) -> List[Tuple[Dict[str, int], int]]:
        if self._cluster_state is None:
            return [({}, 0)]

        state = self._cluster_state

        if state.n_tasks_by_capset:
            raise NotImplementedError("IncrementalRatioEstimator does not support capability-aware scaling")

        manager = state.managers.get(manager_id)
        if manager is None:
            return [({}, 0)]

        return [({}, self._compute_desired(manager, state))]

    def _compute_desired(self, manager: ManagerInfo, state: ClusterState) -> int:
        """Compute desired worker count using incremental scaling logic."""
        managed_worker_count = manager.worker_count

        if state.n_workers == 0:
            desired = managed_worker_count + 1 if state.n_tasks > 0 else managed_worker_count
        else:
            task_ratio = state.n_tasks / state.n_workers
            if task_ratio > self._upper_task_ratio:
                desired = managed_worker_count + 1
            elif task_ratio < self._lower_task_ratio:
                desired = 0 if state.n_tasks == 0 else max(1, ceil(state.n_tasks / self._upper_task_ratio))
            else:
                desired = managed_worker_count

        if manager.max_concurrency is not None:
            desired = min(desired, manager.max_concurrency)

        return max(0, desired)

    def forget(self, manager_id: bytes) -> None:
        pass
