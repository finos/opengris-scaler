import logging
import math
from collections import defaultdict
from typing import Dict, FrozenSet, List, Tuple

import aiohttp
from aiohttp import web

from scaler.protocol.python.message import InformationSnapshot
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerGroupID
from scaler.utility.identifiers import WorkerID


class CapabilityScalingController(ScalingController):
    """
    A scaling controller that scales worker groups based on task-required capabilities.

    When tasks require specific capabilities (e.g., {"gpu": 1}), this controller will
    request worker groups that provide those capabilities from the worker adapter.
    It uses the same task-to-worker ratio logic as VanillaScalingController but applies
    it per capability set.
    """

    def __init__(self, adapter_webhook_url: str):
        self._adapter_webhook_url = adapter_webhook_url
        self._lower_task_ratio = 0.5
        self._upper_task_ratio = 5

        # Track worker groups by their capability set
        # Key: frozenset of capability names (e.g., frozenset({"gpu"}))
        # Value: Dict mapping WorkerGroupID to list of WorkerIDs
        self._worker_groups_by_capability: Dict[FrozenSet[str], Dict[WorkerGroupID, List[WorkerID]]] = defaultdict(dict)

        # Track all worker groups for status reporting
        self._worker_groups: Dict[WorkerGroupID, List[WorkerID]] = {}

    def get_status(self):
        return ScalingManagerStatus.new_msg(worker_groups=self._worker_groups)

    async def on_snapshot(self, snapshot: InformationSnapshot):
        # Group tasks by their required capabilities
        tasks_by_capability = self._group_tasks_by_capability(snapshot)

        # Group workers by their provided capabilities
        workers_by_capability = self._group_workers_by_capability(snapshot)

        # Handle scaling for each capability set
        await self._handle_capability_scaling(snapshot, tasks_by_capability, workers_by_capability)

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
            # Worker can handle task if task capabilities are subset of worker capabilities
            if required_capabilities <= worker_capability_keys:
                capable_workers.extend(workers)

        return capable_workers

    async def _handle_capability_scaling(
        self,
        information_snapshot: InformationSnapshot,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]],
    ):
        """Handle scaling decisions for each capability set."""

        # Complexity: O(C_t * C_w * W) where C_t is the number of distinct task capability sets,
        # C_w is the number of distinct worker capability sets, and W is the total number of workers.
        # This arises from calling _find_capable_workers for each task capability set.

        # Process each capability set that has pending tasks
        for capability_keys, tasks in tasks_by_capability.items():
            # Find workers that can handle these tasks
            capable_workers = self._find_capable_workers(capability_keys, workers_by_capability)

            # Get a representative capability dict for starting new workers
            # Use the first task's capabilities as the template
            if not tasks:
                logging.warning(f"No tasks found for capability set {capability_keys}")
                continue
            capability_dict = tasks[0]

            await self._scale_for_capability(capability_keys, capability_dict, len(tasks), capable_workers)

        # Check for idle capability-specific worker groups that should be shut down
        await self._check_idle_worker_groups(information_snapshot, tasks_by_capability, workers_by_capability)

    async def _scale_for_capability(
        self,
        capability_keys: FrozenSet[str],
        capability_dict: Dict[str, int],
        task_count: int,
        capable_workers: List[Tuple[WorkerID, int]],
    ):
        """Apply scaling logic for a specific capability set."""
        worker_count = len(capable_workers)

        if worker_count == 0 and task_count > 0:
            # No capable workers yet - start a group if one isn't already pending
            if not self._has_capable_worker_group(capability_keys):
                await self._start_worker_group(capability_keys, capability_dict)
            return

        # Scale up if task ratio exceeds threshold
        task_ratio = task_count / worker_count
        if task_ratio > self._upper_task_ratio:
            await self._start_worker_group(capability_keys, capability_dict)

    def _has_capable_worker_group(self, required_capabilities: FrozenSet[str]) -> bool:
        """
        Check if we have already started a worker group that can handle tasks
        with the given required capabilities.
        """
        for group_capability_keys, worker_groups in self._worker_groups_by_capability.items():
            if worker_groups and required_capabilities <= group_capability_keys:
                return True
        return False

    async def _check_idle_worker_groups(
        self,
        information_snapshot: InformationSnapshot,
        tasks_by_capability: Dict[FrozenSet[str], List[Dict[str, int]]],
        workers_by_capability: Dict[FrozenSet[str], List[Tuple[WorkerID, int]]],
    ):
        """Check for and shut down idle worker groups."""

        # Complexity: O(C^2 * (T + W)) where C is the number of distinct capability sets,
        # T is the total number of tasks, and W is the total number of workers.
        # For each tracked capability set, we iterate over all task capability sets to count
        # matching tasks, and call _find_capable_workers which iterates over worker capability sets.
        # This could be optimized if it becomes a performance bottleneck.

        for capability_keys, worker_group_dict in list(self._worker_groups_by_capability.items()):
            if not worker_group_dict:
                continue

            # Find tasks that these workers can handle
            task_count = 0
            for task_capability_keys, tasks in tasks_by_capability.items():
                if task_capability_keys <= capability_keys:
                    task_count += len(tasks)

            # Find capable workers for this capability set
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

                await self._shutdown_worker_group(capability_keys, least_busy_group_id)

    async def _start_worker_group(self, capability_keys: FrozenSet[str], capability_dict: Dict[str, int]):
        """Start a new worker group with the specified capabilities."""
        response, status = await self._make_request({"action": "get_worker_adapter_info"})
        if status != web.HTTPOk.status_code:
            logging.warning("Failed to get worker adapter info.")
            return

        # Count total worker groups across all capabilities
        total_worker_groups = sum(len(groups) for groups in self._worker_groups_by_capability.values())
        if total_worker_groups >= response.get("max_worker_groups", math.inf):
            return

        # Include capabilities in the request
        request_payload = {"action": "start_worker_group", "capabilities": capability_dict}
        logging.info(f"Requesting worker group with capabilities: {capability_dict!r}")

        response, status = await self._make_request(request_payload)
        if status == web.HTTPTooManyRequests.status_code:
            logging.warning("Capacity exceeded, cannot start new worker group.")
            return
        if status == web.HTTPInternalServerError.status_code:
            logging.error(f"Failed to start worker group: {response.get('error', 'Unknown error')}")
            return

        worker_group_id = response["worker_group_id"].encode()
        worker_ids = [WorkerID(worker_id.encode()) for worker_id in response["worker_ids"]]

        # Get actual capabilities from the worker adapter response
        actual_capabilities = response.get("capabilities", capability_dict)
        actual_capability_keys = frozenset(actual_capabilities.keys())

        # Track the worker group by the actual capabilities provided by the worker adapter
        self._worker_groups_by_capability[actual_capability_keys][worker_group_id] = worker_ids
        self._worker_groups[worker_group_id] = worker_ids

        logging.info(f"Started worker group: {worker_group_id.decode()} with capabilities: {actual_capabilities!r}")

    async def _shutdown_worker_group(self, capability_keys: FrozenSet[str], worker_group_id: WorkerGroupID):
        """Shut down a worker group."""
        if worker_group_id not in self._worker_groups:
            logging.error(f"Worker group with ID {worker_group_id.decode()} does not exist.")
            return

        response, status = await self._make_request(
            {"action": "shutdown_worker_group", "worker_group_id": worker_group_id.decode()}
        )
        if status == web.HTTPNotFound.status_code:
            logging.error(f"Worker group with ID {worker_group_id.decode()} not found in adapter.")
            return
        if status == web.HTTPInternalServerError.status_code:
            logging.error(f"Failed to shutdown worker group: {response.get('error', 'Unknown error')}")
            return

        # Remove from tracking
        if capability_keys in self._worker_groups_by_capability:
            self._worker_groups_by_capability[capability_keys].pop(worker_group_id, None)
            if not self._worker_groups_by_capability[capability_keys]:
                del self._worker_groups_by_capability[capability_keys]

        self._worker_groups.pop(worker_group_id, None)
        logging.info(f"Shutdown worker group: {worker_group_id.decode()}")

    async def _make_request(self, payload):
        """Make an HTTP request to the worker adapter."""
        async with aiohttp.ClientSession() as session:
            async with session.post(self._adapter_webhook_url, json=payload) as response:
                return await response.json(), response.status
