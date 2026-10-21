from math import ceil
from typing import Dict, List, Optional, Tuple

from scaler.scheduler.policies.scaling.estimator import ClusterState, ScalingEstimator


class RatioEstimator(ScalingEstimator):
    """Ratio-based estimator that computes desired counts from task ratios.

    Computes desired = ceil(tasks / upper_task_ratio) per capability set, with a minimum of one worker per non-empty
    capability set.
    """

    def __init__(self, upper_task_ratio: int = 5):
        self._upper_task_ratio = upper_task_ratio
        self._cluster_state: Optional[ClusterState] = None

    def update(self, cluster_state: ClusterState) -> None:
        self._cluster_state = cluster_state

    def estimate(self, manager_id: bytes) -> List[Tuple[Dict[str, int], int]]:
        if self._cluster_state is None:
            return []

        state = self._cluster_state

        if state.n_tasks_by_capset:
            capsets = state.n_tasks_by_capset
        else:
            capsets = {frozenset(): state.n_tasks}

        result: List[Tuple[Dict[str, int], int]] = []
        for capset, task_count in capsets.items():
            if task_count == 0:
                continue

            desired = max(1, ceil(task_count / self._upper_task_ratio))

            if state.max_concurrency is not None:
                desired = min(desired, state.max_concurrency)

            capabilities = state.capset_representatives.get(capset, {})
            result.append((capabilities, max(0, desired)))

        return result

    def forget(self, manager_id: bytes) -> None:
        pass
