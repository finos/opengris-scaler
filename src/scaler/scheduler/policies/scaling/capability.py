import logging
from math import ceil
from typing import Dict, FrozenSet, List, Mapping, Tuple

from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.utility import build_set_desired_command, forget_departed_managers
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerManagerID

logger = logging.getLogger(__name__)


class CapabilityScalingPolicy(ScalingPolicy):
    """
    A scaling policy that scales workers based on task-required capabilities.

    For each distinct capability set observed in pending tasks, it computes a desired worker
    count using a task-to-worker ratio threshold. The desired counts are sent declaratively
    via setDesiredTaskConcurrency; the worker manager is responsible for making it so.

    Decisions depend only on the current observation. The last counts logged per manager are kept for
    logging.
    """

    def __init__(self):
        self._upper_task_ratio = 5
        self._logged_desired_by_manager: Dict[WorkerManagerID, Dict[FrozenSet[Tuple[str, int]], int]] = {}

    def get_scaling_commands(
        self, worker_manager_id: WorkerManagerID, cluster_state: ClusterState
    ) -> List[WorkerManagerCommand]:
        forget_departed_managers(self._logged_desired_by_manager, set(cluster_state.managers))

        info = cluster_state.managers[worker_manager_id]
        desired_per_capset = self._compute_desired_per_capset(cluster_state, info)
        self.__log_decisions(worker_manager_id, cluster_state, desired_per_capset)
        return [build_set_desired_command(desired_per_capset)]

    def __log_decisions(
        self,
        manager_id: WorkerManagerID,
        cluster_state: ClusterState,
        desired_per_capset: List[Tuple[Mapping[str, int], int]],
    ) -> None:
        """Logs what is asked of a manager, once per run of the same request.

        The request repeats every heartbeat until the manager reaches the count, and forever if it cannot.
        """

        previously_logged = self._logged_desired_by_manager.get(manager_id, {})
        self._logged_desired_by_manager[manager_id] = {
            frozenset(capabilities.items()): desired for capabilities, desired in desired_per_capset
        }

        for capabilities, desired in desired_per_capset:
            if previously_logged.get(frozenset(capabilities.items())) == desired:
                continue

            task_count = cluster_state.n_tasks_by_capset[frozenset(capabilities.keys())]
            logger.info(f"scaling {manager_id!r}: tasks={task_count}, capabilities={capabilities} -> desired={desired}")

    def _compute_desired_per_capset(
        self, cluster_state: ClusterState, manager_info: WorkerManagerInfo
    ) -> List[Tuple[Mapping[str, int], int]]:
        """Compute desired worker count per capability set from the cluster state.

        Capsets with zero tasks are omitted (declarative "no opinion" for that capset).
        Each desired count is clamped by the manager's max_task_concurrency.
        """
        result: List[Tuple[Mapping[str, int], int]] = []
        for capset, count in cluster_state.n_tasks_by_capset.items():
            if count == 0:
                continue
            desired = max(1, ceil(count / self._upper_task_ratio))
            if manager_info.max_task_concurrency is not None:
                desired = min(desired, manager_info.max_task_concurrency)
            result.append((cluster_state.capset_representatives[capset], desired))
        return result
