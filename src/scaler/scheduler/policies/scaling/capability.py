import logging
from collections import defaultdict
from math import ceil
from typing import Dict, FrozenSet, List, Tuple

from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.utility import build_set_desired_command, forget_departed_managers
from scaler.scheduler.types import WorkerManagerInfo
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.snapshot import InformationSnapshot

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
        self,
        worker_manager_id: WorkerManagerID,
        information_snapshot: InformationSnapshot,
        worker_manager_infos: Dict[WorkerManagerID, WorkerManagerInfo],
        worker_count_by_manager: Dict[WorkerManagerID, int],
    ) -> List[WorkerManagerCommand]:
        forget_departed_managers(self._logged_desired_by_manager, set(worker_manager_infos))

        tasks_by_capability = self._group_tasks_by_capability(information_snapshot)
        info = worker_manager_infos[worker_manager_id]
        desired_per_capset = self._compute_desired_per_capset(tasks_by_capability, info)
        self.__log_decisions(worker_manager_id, tasks_by_capability, desired_per_capset)
        return [build_set_desired_command(desired_per_capset)]

    def __log_decisions(
        self,
        manager_id: WorkerManagerID,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        desired_per_capset: List[Tuple[Dict[str, int], int]],
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

            task_count = len(tasks_by_capability[frozenset(capabilities.keys())])
            logger.info(f"scaling {manager_id!r}: tasks={task_count}, capabilities={capabilities} -> desired={desired}")

    def _group_tasks_by_capability(
        self, information_snapshot: InformationSnapshot
    ) -> Dict[FrozenSet[str], List[Dict[str, int]]]:
        """Group pending tasks by their required capability keys."""
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]] = defaultdict(list)

        for task in information_snapshot.tasks.values():
            capability_keys = frozenset(task.capabilities.keys())
            tasks_by_capability[capability_keys].append(task.capabilities)

        return tasks_by_capability

    def _compute_desired_per_capset(
        self, tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]], manager_info: WorkerManagerInfo
    ) -> List[Tuple[Dict[str, int], int]]:
        """Compute desired worker count per capability set from observed tasks.

        Capsets with zero tasks are omitted (declarative "no opinion" for that capset).
        Each desired count is clamped by the manager's max_task_concurrency.
        """
        max_concurrency = manager_info.max_task_concurrency
        result: List[Tuple[Dict[str, int], int]] = []
        for _capability_keys, tasks in tasks_by_capability.items():
            if not tasks:
                continue
            desired = max(1, ceil(len(tasks) / self._upper_task_ratio))
            if max_concurrency != -1:
                desired = min(desired, max_concurrency)
            # Use first task's concrete capability dict as the representative for the capset.
            result.append((tasks[0], desired))
        return result
