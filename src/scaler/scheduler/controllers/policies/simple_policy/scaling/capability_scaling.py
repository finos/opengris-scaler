from collections import defaultdict
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

    For each distinct capability set observed in pending tasks, it asks for one worker per task,
    bounded by the manager's maximum task concurrency. The desired counts are sent declaratively
    via setDesiredTaskConcurrency; the worker manager is responsible for making it so.
    """

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

        A worker runs one task at a time, so the worker count is the cluster's parallelism: ask for
        one worker per outstanding task. maxTaskConcurrency is what bounds that, and asking for less
        than it while tasks are outstanding leaves capacity the operator paid for standing idle.

        Capsets with zero tasks are omitted (declarative "no opinion" for that capset).
        """
        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        result: List[Tuple[Dict[str, int], int]] = []
        for _capability_keys, tasks in tasks_by_capability.items():
            if not tasks:
                continue
            desired = len(tasks)
            if max_concurrency != -1:
                desired = min(desired, max_concurrency)
            # Use first task's concrete capability dict as the representative for the capset.
            result.append((tasks[0], desired))
        return result
