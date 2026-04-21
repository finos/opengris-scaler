import logging
from collections import defaultdict
from math import ceil
from typing import Dict, FrozenSet, List, Optional, Tuple

from scaler.protocol.capnp import (
    ScalingManagerStatus,
    WorkerManagerCommand,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
)
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.worker_manager_utilties import build_scaling_manager_status
from scaler.utility.identifiers import WorkerID
from scaler.utility.snapshot import InformationSnapshot


class CapabilityScalingPolicy(ScalingPolicy):
    """
    A stateless scaling policy that scales workers based on task-required capabilities.

    When tasks require specific capabilities (e.g., {"gpu": 1}), this policy will
    request workers that provide those capabilities from the worker manager.
    It uses the same task-to-worker ratio logic as VanillaScalingPolicy but applies
    it per capability set.

    All state (managed_worker_ids, managed_worker_capabilities) is passed in as parameters.
    """

    def __init__(self):
        self._lower_task_ratio = 0.5
        self._upper_task_ratio = 5

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        managed_worker_capabilities: Dict[str, int],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        # Group tasks by their required capabilities
        tasks_by_capability = self._group_tasks_by_capability(information_snapshot)

        # Group workers by their provided capabilities
        workers_by_capability = self._group_workers_by_capability(information_snapshot)

        snapshot = worker_manager_snapshots.get(worker_manager_heartbeat.workerManagerID)
        pending = snapshot.pending_worker_count if snapshot else 0

        # Try to get start commands first - if any, return early
        start_commands = self._get_start_commands(
            tasks_by_capability,
            workers_by_capability,
            managed_worker_ids,
            managed_worker_capabilities,
            worker_manager_heartbeat,
            pending,
        )
        if start_commands:
            return start_commands

        # Otherwise check for shutdown commands
        return self._get_shutdown_commands(
            information_snapshot, tasks_by_capability, workers_by_capability, managed_worker_ids
        )

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

    def _group_workers_by_capability(
        self, information_snapshot: InformationSnapshot
    ) -> Dict[FrozenSet[str], List[Tuple[WorkerID, int]]]:
        """
        Group workers by their provided capability keys.
        Returns a dict mapping capability set to list of (worker_id, queued_tasks).
        """
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]] = defaultdict(list)

        for worker_id, worker_heartbeat in information_snapshot.workers.items():
            capability_keys = frozenset(worker_heartbeat.capabilities.keys())
            workers_by_capability[capability_keys].append((worker_id, worker_heartbeat.queuedTasks))

        return workers_by_capability

    def _find_capable_workers(
        self,
        required_capabilities: FrozenSet[str],
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]],
    ) -> List[Tuple[WorkerID, int]]:
        """
        Find all workers that can handle tasks with the given required capabilities.
        A worker can handle a task if the task's capability keys are a subset of the worker's.
        """
        capable_workers: List[Tuple[WorkerID, int]] = []

        for worker_capability_keys, workers in workers_by_capability.items():
            if required_capabilities <= worker_capability_keys:
                capable_workers.extend(workers)

        return capable_workers

    def _get_start_commands(
        self,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]],
        managed_worker_ids: List[WorkerID],
        managed_worker_capabilities: Dict[str, int],
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        pending_worker_count: int = 0,
    ) -> List[WorkerManagerCommand]:
        """Collect all start commands for capability sets that need scaling up."""
        commands: List[WorkerManagerCommand] = []

        for capability_keys, tasks in tasks_by_capability.items():
            if not tasks:
                continue

            capable_workers = self._find_capable_workers(capability_keys, workers_by_capability)
            capability_dict = tasks[0]
            worker_count = len(capable_workers)
            task_count = len(tasks)

            if worker_count == 0 and task_count > 0:
                if not self._has_capable_managed_workers(
                    capability_keys, managed_worker_ids, managed_worker_capabilities
                ):
                    command = self._create_start_command(
                        capability_dict, managed_worker_ids, worker_manager_heartbeat, pending_worker_count
                    )
                    if command is not None:
                        commands.append(command)
            elif worker_count > 0:
                task_ratio = task_count / worker_count
                if task_ratio > self._upper_task_ratio:
                    command = self._create_start_command(
                        capability_dict, managed_worker_ids, worker_manager_heartbeat, pending_worker_count
                    )
                    if command is not None:
                        commands.append(command)

        return commands

    def _get_shutdown_commands(
        self,
        information_snapshot: InformationSnapshot,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]],
        managed_worker_ids: List[WorkerID],
    ) -> List[WorkerManagerCommand]:
        """Collect all shutdown commands for idle workers greedily."""
        # Complexity: O(C^2 * (T + W)) where C is the number of distinct capability sets,
        # T is the total number of tasks, and W is the total number of workers.
        # For each tracked capability set, we iterate over all task capability sets to count
        # matching tasks, and call _find_capable_workers which iterates over worker capability sets.
        # This could be optimized if it becomes a performance bottleneck.

        managed_set = set(managed_worker_ids)
        shutdown_ids: List[bytes] = []

        for capability_keys in list(workers_by_capability.keys()):
            capable_workers = self._find_capable_workers(capability_keys, workers_by_capability)
            worker_count = len(capable_workers)
            if worker_count == 0:
                continue

            task_count = 0
            for task_capability_keys, tasks in tasks_by_capability.items():
                if task_capability_keys <= capability_keys:
                    task_count += len(tasks)

            if task_count / worker_count < self._lower_task_ratio:
                managed_capable = [(wid, queued) for wid, queued in capable_workers if wid in managed_set]
                managed_capable.sort(key=lambda x: x[1])

                if task_count == 0:
                    min_keep = 0
                else:
                    min_keep = max(1, ceil(task_count / self._upper_task_ratio))

                # Only shut down managed workers; non-managed workers count toward min_keep
                non_managed_count = worker_count - len(managed_capable)
                managed_to_keep = max(0, min_keep - non_managed_count)
                to_shutdown = len(managed_capable) - managed_to_keep

                for wid, _ in managed_capable[:to_shutdown]:
                    shutdown_ids.append(bytes(wid))

        if not shutdown_ids:
            return []

        return [
            WorkerManagerCommand(
                workerIDs=shutdown_ids, command=WorkerManagerCommandType.shutdownWorkers, capabilities={}
            )
        ]

    def _has_capable_managed_workers(
        self,
        required_capabilities: FrozenSet[str],
        managed_worker_ids: List[WorkerID],
        managed_worker_capabilities: Dict[str, int],
    ) -> bool:
        """
        Check if we already have managed workers that can handle tasks
        with the given required capabilities.
        """
        if not managed_worker_ids:
            return False
        manager_capability_keys = frozenset(managed_worker_capabilities.keys())
        return required_capabilities <= manager_capability_keys

    def _create_start_command(
        self,
        capability_dict: Dict[str, int],
        managed_worker_ids: List[WorkerID],
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        pending_worker_count: int = 0,
    ) -> Optional[WorkerManagerCommand]:
        """Create a start workers command if capacity allows."""
        max_concurrency = worker_manager_heartbeat.maxTaskConcurrency
        if max_concurrency != -1 and len(managed_worker_ids) + pending_worker_count >= max_concurrency:
            return None

        logging.info(f"Requesting worker with capabilities: {capability_dict!r}")
        return WorkerManagerCommand(
            workerIDs=[], command=WorkerManagerCommandType.startWorkers, capabilities=capability_dict
        )
