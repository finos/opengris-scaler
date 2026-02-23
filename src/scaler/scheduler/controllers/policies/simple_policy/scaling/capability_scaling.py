import logging
from collections import defaultdict
from typing import Dict, FrozenSet, List, Optional, Tuple

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import (
    WorkerGroupCapabilities,
    WorkerGroupID,
    WorkerGroupState,
)
from scaler.utility.identifiers import WorkerID


class CapabilityScalingController(ScalingController):
    """
    A stateless scaling controller that scales worker groups based on task-required capabilities.

    When tasks require specific capabilities (e.g., {"gpu": 1}), this controller will
    request worker groups that provide those capabilities from the worker adapter.
    It uses the same task-to-worker ratio logic as VanillaScalingController but applies
    it per capability set.

    All state (worker_groups, worker_group_capabilities) is passed in as parameters.
    """

    def __init__(self):
        self._lower_task_ratio = 0.5
        self._upper_task_ratio = 5

    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
    ) -> List[WorkerAdapterCommand]:
        # Derive worker_groups_by_capability from worker_groups + worker_group_capabilities
        worker_groups_by_capability = self._derive_worker_groups_by_capability(worker_groups, worker_group_capabilities)

        # Group tasks by their required capabilities
        tasks_by_capability = self._group_tasks_by_capability(information_snapshot)

        # Group workers by their provided capabilities
        workers_by_capability = self._group_workers_by_capability(information_snapshot)

        # Try to get start commands first - if any, return early
        start_commands = self._get_start_commands(
            tasks_by_capability, workers_by_capability, worker_groups_by_capability, adapter_heartbeat
        )
        if start_commands:
            return start_commands

        # Otherwise check for shutdown commands
        return self._get_shutdown_commands(
            information_snapshot, tasks_by_capability, workers_by_capability, worker_groups_by_capability
        )

    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups=worker_groups)

    def _derive_worker_groups_by_capability(
        self, worker_groups: WorkerGroupState, worker_group_capabilities: WorkerGroupCapabilities
    ) -> Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]]:
        """Derive worker_groups_by_capability from worker_groups and worker_group_capabilities."""
        result: Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]] = defaultdict(dict)
        for worker_group_id, worker_ids in worker_groups.items():
            caps = worker_group_capabilities.get(worker_group_id, {})
            capability_keys = frozenset(caps.keys())
            result[capability_keys][worker_group_id] = worker_ids
        return result

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
            workers_by_capability[capability_keys].append((worker_id, worker_heartbeat.queued_tasks))

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
        worker_groups_by_capability: Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]],
        adapter_heartbeat: WorkerAdapterHeartbeat,
    ) -> List[WorkerAdapterCommand]:
        """Collect all start commands for capability sets that need scaling up."""
        commands: List[WorkerAdapterCommand] = []

        for capability_keys, tasks in tasks_by_capability.items():
            if not tasks:
                continue

            capable_workers = self._find_capable_workers(capability_keys, workers_by_capability)
            capability_dict = tasks[0]
            worker_count = len(capable_workers)
            task_count = len(tasks)

            if worker_count == 0 and task_count > 0:
                if not self._has_capable_worker_group(capability_keys, worker_groups_by_capability):
                    command = self._create_start_command(
                        capability_dict, worker_groups_by_capability, adapter_heartbeat
                    )
                    if command is not None:
                        commands.append(command)
            elif worker_count > 0:
                task_ratio = task_count / worker_count
                if task_ratio > self._upper_task_ratio:
                    command = self._create_start_command(
                        capability_dict, worker_groups_by_capability, adapter_heartbeat
                    )
                    if command is not None:
                        commands.append(command)

        return commands

    def _get_shutdown_commands(
        self,
        information_snapshot: InformationSnapshot,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]],
        worker_groups_by_capability: Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]],
    ) -> List[WorkerAdapterCommand]:
        """Collect all shutdown commands for idle worker groups."""
        # Complexity: O(C^2 * (T + W)) where C is the number of distinct capability sets,
        # T is the total number of tasks, and W is the total number of workers.
        # For each tracked capability set, we iterate over all task capability sets to count
        # matching tasks, and call _find_capable_workers which iterates over worker capability sets.
        # This could be optimized if it becomes a performance bottleneck.
        commands: List[WorkerAdapterCommand] = []

        for capability_keys, worker_group_dict in list(worker_groups_by_capability.items()):
            if not worker_group_dict:
                continue

            # Find tasks that these workers can handle
            task_count = 0
            for task_capability_keys, tasks in tasks_by_capability.items():
                if task_capability_keys <= capability_keys:
                    task_count += len(tasks)

            capable_workers = self._find_capable_workers(capability_keys, workers_by_capability)
            worker_count = len(capable_workers)

            if worker_count == 0:
                continue

            task_ratio = task_count / worker_count
            if task_ratio < self._lower_task_ratio:
                # Find the worker group with the least queued tasks
                worker_group_task_counts: Dict[WorkerGroupID, int] = {}
                for worker_group_id, worker_ids in worker_group_dict.items():
                    total_queued = sum(
                        information_snapshot.workers[worker_id].queued_tasks
                        for worker_id in worker_ids
                        if worker_id in information_snapshot.workers
                    )
                    worker_group_task_counts[worker_group_id] = total_queued

                if not worker_group_task_counts:
                    continue

                # Select the worker group with the fewest queued tasks to shut down
                least_busy_group_id = min(worker_group_task_counts, key=lambda gid: worker_group_task_counts[gid])

                # Don't scale down if there are pending tasks and this would leave no capable workers
                workers_in_group = len(worker_group_dict.get(least_busy_group_id, []))
                remaining_worker_count = worker_count - workers_in_group
                if task_count > 0 and remaining_worker_count == 0:
                    # This is the last worker group that can handle these tasks - don't shut it down
                    continue
                if remaining_worker_count > 0 and (task_count / remaining_worker_count) > self._upper_task_ratio:
                    # Shutting down this group would cause task ratio to exceed upper threshold and scale-up again
                    continue

                commands.append(
                    WorkerAdapterCommand.new_msg(
                        worker_group_id=least_busy_group_id, command=WorkerAdapterCommandType.ShutdownWorkerGroup
                    )
                )

        return commands

    def _has_capable_worker_group(
        self,
        required_capabilities: FrozenSet[str],
        worker_groups_by_capability: Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]],
    ) -> bool:
        """
        Check if we have already started a worker group that can handle tasks
        with the given required capabilities.
        """
        for group_capability_keys, worker_groups in worker_groups_by_capability.items():
            if worker_groups and required_capabilities <= group_capability_keys:
                return True
        return False

    def _create_start_command(
        self,
        capability_dict: Dict[str, int],
        worker_groups_by_capability: Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]],
        adapter_heartbeat: WorkerAdapterHeartbeat,
    ) -> Optional[WorkerAdapterCommand]:
        """Create a start worker group command if capacity allows."""
        total_worker_groups = sum(len(groups) for groups in worker_groups_by_capability.values())
        if total_worker_groups >= adapter_heartbeat.max_worker_groups:
            return None

        logging.info(f"Requesting worker group with capabilities: {capability_dict!r}")
        return WorkerAdapterCommand.new_msg(
            worker_group_id=b"", command=WorkerAdapterCommandType.StartWorkerGroup, capabilities=capability_dict
        )
