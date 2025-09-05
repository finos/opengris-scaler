import logging
import uuid
from typing import Dict, Set

import aiohttp

from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import StateTask, StateWorker
from scaler.scheduler.controllers.mixins import ScalingController
from scaler.utility.identifiers import TaskID, WorkerID
from scaler.utility.mixins import Reporter


class NullScalingController(ScalingController, Reporter):
    """No-op scaling controller used when scaling is disabled.
    """

    def get_status(self):
        """Return the status.

        :returns: Dictionary with worker/task information.
        :rtype: dict
        """
        return {"worker_task_counts": {}, "workers_pending_startup": [], "workers_pending_shutdown": []}


class VanillaScalingController(ScalingController, Reporter):
    """Simple autoscaling controller based on tasks-per-worker ratio.

    :param adapter_webhook_url: Adapter webhook URL used to request worker start/stop.
    :param lower_task_ratio: Lower bound of tasks per worker before scaling down.
    :param upper_task_ratio: Upper bound of tasks per worker before scaling up.
    """

    def __init__(self, adapter_webhook_url: str, lower_task_ratio: int = 1, upper_task_ratio: int = 10):
        self.inactive_tasks: Set[TaskID] = set()
        self.task_to_worker: Dict[TaskID, WorkerID] = {}
        self.worker_to_tasks: Dict[WorkerID, Set[TaskID]] = {}

        self.workers_pending_startup: Set[WorkerID] = set()
        self.workers_pending_shutdown: Set[WorkerID] = set()

        self.adapter_webhook_url: str = adapter_webhook_url
        self.lower_task_ratio: int = lower_task_ratio
        self.upper_task_ratio: int = upper_task_ratio

    def get_status(self):
        """Return the status.

        :returns: Dictionary with worker/task information.
        :rtype: dict
        """
        return {
            "worker_task_counts": {worker_id.decode(): len(tasks) for worker_id, tasks in self.worker_to_tasks.items()},
            "workers_pending_startup": [worker_id.decode() for worker_id in self.workers_pending_startup],
            "workers_pending_shutdown": [worker_id.decode() for worker_id in self.workers_pending_shutdown],
        }

    async def on_state_worker(self, state_worker: StateWorker):
        """Handle worker state updates.

        Removes worker ids from pending sets when the worker reports a matching
        connected/disconnected state.

        :param state_worker: StateWorker message with worker_id and message payload.
        :type state_worker: StateWorker
        """
        if state_worker.message == b"connected" and state_worker.worker_id in self.workers_pending_startup:
            self.workers_pending_startup.remove(state_worker.worker_id)

        elif state_worker.message == b"disconnected" and state_worker.worker_id in self.workers_pending_shutdown:
            self.workers_pending_shutdown.remove(state_worker.worker_id)

    async def on_state_task(self, state_task: StateTask):
        """Handle task lifecycle updates and perform scaling decisions.

        Behaviour:
        - Track inactive tasks and ensure at least one worker exists when tasks appear.
        - Assign running tasks to workers and maintain reverse mappings.
        - When tasks complete/failed/canceled, remove mappings and evaluate scaling.

        :param state_task: StateTask message describing task id, status, and worker (if running).
        :type state_task: StateTask
        """
        if state_task.status == TaskStatus.Inactive:
            if len(self.worker_to_tasks) == 0:
                try:
                    await self.start_worker()
                except Exception as e:
                    logging.error("Failed to start new worker: %s", e)

            self.inactive_tasks.add(state_task.task_id)
            return

        if state_task.status == TaskStatus.Running:
            self.inactive_tasks.remove(state_task.task_id)

            worker = state_task.worker
            old_worker = self.task_to_worker.get(state_task.task_id, None)
            self.task_to_worker[state_task.task_id] = worker
            self.worker_to_tasks[worker].add(state_task.task_id)

            # In the case of a reroute, discard any leftover tasks
            if old_worker is not None and old_worker in self.worker_to_tasks:
                self.worker_to_tasks[old_worker].discard(state_task.task_id)

        else:
            worker = self.task_to_worker[state_task.task_id]

        if state_task.status in (TaskStatus.Success, TaskStatus.Failed, TaskStatus.Canceled):
            self.task_to_worker.pop(state_task.task_id)

            # The worker may be removed beforehand so check if the worker exists
            if worker in self.worker_to_tasks:
                self.worker_to_tasks[worker].discard(state_task.task_id)

        else:
            return

        total_tasks = len(self.task_to_worker) + len(self.inactive_tasks)
        task_ratio = total_tasks / len(self.worker_to_tasks) if len(self.worker_to_tasks) > 0 else float("inf")

        if task_ratio > self.upper_task_ratio:
            try:
                await self.start_worker()
            except Exception as e:
                logging.error("Failed to start new worker: %s", e)
                return

            logging.info("Start new worker as task ratio is above the upper threshold.")

        elif task_ratio < self.lower_task_ratio:
            if total_tasks > 0 and len(self.worker_to_tasks) <= 1:
                return

            worker_id = min(self.worker_to_tasks, key=lambda x: len(self.worker_to_tasks.get(x)))

            try:
                await self.shutdown_worker(worker_id)
            except Exception as e:
                logging.error("Failed to shutdown worker %s: %s", worker_id.decode(), e)
                return

            logging.info("Shutdown worker %s as task ratio is below the lower threshold.", worker_id)

    async def start_worker(self) -> WorkerID:
        """Request the worker_adapter to start a new worker and register it as pending.

        :returns: Identifier of the newly created worker.
        :rtype: WorkerID
        """
        worker_id_str = f"worker-{uuid.uuid4().hex}"
        response = await self._make_request({"action": "start_worker", "worker_id": worker_id_str})

        worker_id = WorkerID(response["worker_id"].encode())

        self.workers_pending_startup.add(worker_id)
        self.worker_to_tasks[worker_id] = set()

        return worker_id

    async def shutdown_worker(self, worker_id: WorkerID):
        """Request the worker_adapter to shutdown a worker and mark it pending shutdown.

        The worker is removed from local tracking immediately; its disconnected state
        will be reconciled when a StateWorker disconnected message is received.

        :param worker_id: Identifier of the worker to shutdown.
        """
        await self._make_request({"action": "shutdown_worker", "worker_id": worker_id.decode()})

        self.workers_pending_shutdown.add(worker_id)
        self.worker_to_tasks.pop(worker_id)

    async def _make_request(self, payload):
        """POST a JSON payload to the configured worker_adapter webhook and return JSON.

        :param payload: JSON-serializable payload to send to the worker_adapter.
        :raises Exception: If the worker_adapter responds with a non-200 status; the worker_adapter's
            error message is extracted from the response JSON under the "error" key.
        :returns: Decoded JSON response from the worker_adapter.
        :rtype: dict
        """
        async with aiohttp.ClientSession() as session:
            async with session.post(self.adapter_webhook_url, json=payload) as response:
                if response.status == 200:
                    return await response.json()
                raise Exception((await response.json())["error"])
