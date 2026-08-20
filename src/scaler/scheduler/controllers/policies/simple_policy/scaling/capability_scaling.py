from collections import defaultdict
from math import ceil
from typing import Dict, FrozenSet, List, Tuple

from scaler.protocol.capnp import ScalingManagerStatus, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status, build_set_desired_command
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class CapabilityScalingPolicy(ScalingPolicy):
    """
    A stateless scaling policy that scales workers based on task-required capabilities.

    For each distinct capability set observed in pending tasks, it computes a desired worker
    count using a task-to-worker ratio threshold. The desired counts are sent declaratively
    via setDesiredTaskConcurrency; the worker manager is responsible for making it so.
    """

    def __init__(self):
        self._upper_task_ratio = 5

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        tasks_by_capability = self._group_tasks_by_capability(information_snapshot)
        desired_per_capset = self._compute_desired_per_capset(tasks_by_capability, worker_manager_heartbeat)
        return [build_set_desired_command(desired_per_capset)]

    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        return build_scaling_manager_status(managed_workers)

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
        self,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        worker_manager_heartbeat: WorkerManagerHeartbeat,
    ) -> List[Tuple[Dict[str, int], int]]:
        """Compute desired worker count per capability set from observed tasks.

        Capsets with zero tasks are omitted (declarative "no opinion" for that capset).
        Each desired count is clamped by the manager's maxTaskConcurrency.
        """
        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        result: List[Tuple[Dict[str, int], int]] = []
        for _capability_keys, tasks in tasks_by_capability.items():
            if not tasks:
                continue
            desired = min(max(1, ceil(len(tasks) / self._upper_task_ratio)), max_concurrency)
            # Use first task's concrete capability dict as the representative for the capset.
            result.append((tasks[0], desired))
        return result
