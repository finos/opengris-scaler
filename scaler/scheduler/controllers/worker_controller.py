import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.async_connector_zmq import AsyncConnectorZMQ
from scaler.io.config import DEFAULT_PER_WORKER_QUEUE_SIZE
from scaler.protocol.python.common import ObjectStorageAddress, TaskStatus
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    StateWorker,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.status import ProcessorStatus, Resource, WorkerManagerStatus, WorkerStatus
from scaler.scheduler.allocate_policy.mixins import TaskAllocatePolicy
from scaler.scheduler.controllers.mixins import TaskController, WorkerController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter

UINT8_MAX = 2**8 - 1


class VanillaWorkerController(WorkerController, Looper, Reporter):
    def __init__(
        self, timeout_seconds: int, task_allocate_policy: TaskAllocatePolicy, storage_address: ObjectStorageAddress
    ):
        self._timeout_seconds = timeout_seconds
        self._storage_address = storage_address

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnectorZMQ] = None
        self._task_controller: Optional[TaskController] = None

        self._worker_alive_since: Dict[WorkerID, Tuple[float, WorkerHeartbeat]] = dict()
        self._allocator_policy = task_allocate_policy

    def register(self, binder: AsyncBinder, binder_monitor: AsyncConnectorZMQ, task_controller: TaskController):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._task_controller = task_controller

    async def assign_task_to_worker(self, task: Task) -> bool:
        worker = await self._allocator_policy.assign_task(task)
        if not worker.is_valid():
            return False

        # send to worker
        await self._binder.send(worker, task)
        return True

    async def on_task_cancel(self, task_cancel: TaskCancel):
        worker = self._allocator_policy.remove_task(task_cancel.task_id)
        if not worker.is_valid():
            logging.error(f"cannot find task_id={task_cancel.task_id.hex()} in task workers")
            return

        await self._binder.send(worker, task_cancel)

    async def on_task_result(self, task_result: TaskResult):
        worker = self._allocator_policy.remove_task(task_result.task_id)

        if task_result.status in {TaskStatus.Canceled, TaskStatus.NotFound}:
            if worker.is_valid():
                # The worker canceled the task, but the scheduler still had it queued. Re-route the task to another
                # worker.
                await self.__reroute_tasks([task_result.task_id])
            else:
                await self._task_controller.on_task_done(task_result)

            return

        if not worker.is_valid():
            logging.error(
                f"received unknown task result for task_id={task_result.task_id.hex()}, status={task_result.status} "
                f"might due to worker get disconnected or canceled"
            )
            return

        await self._task_controller.on_task_done(task_result)

    async def on_heartbeat(self, worker_id: WorkerID, info: WorkerHeartbeat):
        # TODO: get worker queue size from worker heartbeat
        if await self._allocator_policy.add_worker(worker_id, info.tags, DEFAULT_PER_WORKER_QUEUE_SIZE):
            logging.info(f"worker {worker_id!r} connected")
            await self._binder_monitor.send(StateWorker.new_msg(worker_id, b"connected"))

        self._worker_alive_since[worker_id] = (time.time(), info)
        await self._binder.send(worker_id, WorkerHeartbeatEcho.new_msg(object_storage_address=self._storage_address))

    async def on_client_shutdown(self, client_id: ClientID):
        for worker in self._allocator_policy.get_worker_ids():
            await self.__shutdown_worker(worker)

    async def on_disconnect(self, worker_id: WorkerID, request: DisconnectRequest):
        await self.__disconnect_worker(request.worker)
        await self._binder.send(worker_id, DisconnectResponse.new_msg(request.worker))

    async def routine(self):
        await self.__clean_workers()

    def get_status(self) -> WorkerManagerStatus:
        worker_to_task_numbers = self._allocator_policy.statistics()
        return WorkerManagerStatus.new_msg(
            [
                self.__worker_status_from_heartbeat(worker, worker_to_task_numbers[worker], last, info)
                for worker, (last, info) in self._worker_alive_since.items()
            ]
        )

    @staticmethod
    def __worker_status_from_heartbeat(
        worker_id: WorkerID, worker_task_numbers: Dict, last: float, info: WorkerHeartbeat
    ) -> WorkerStatus:
        current_processor = next((p for p in info.processors if not p.suspended), None)
        suspended = min(len([p for p in info.processors if p.suspended]), UINT8_MAX)
        last_s = min(int(time.time() - last), UINT8_MAX)

        if current_processor:
            debug_info = f"{int(current_processor.initialized)}{int(current_processor.has_task)}{int(info.task_lock)}"
        else:
            debug_info = f"00{int(info.task_lock)}"

        return WorkerStatus.new_msg(
            worker_id=worker_id,
            agent=info.agent,
            rss_free=info.rss_free,
            free=worker_task_numbers["free"],
            sent=worker_task_numbers["sent"],
            queued=info.queued_tasks,
            suspended=suspended,
            lag_us=info.latency_us,
            last_s=last_s,
            itl=debug_info,
            processor_statuses=[
                ProcessorStatus.new_msg(
                    pid=p.pid,
                    initialized=p.initialized,
                    has_task=p.has_task,
                    suspended=p.suspended,
                    resource=Resource.new_msg(p.resource.cpu, p.resource.rss),
                )
                for p in info.processors
            ],
        )

    def has_available_worker(self) -> bool:
        return self._allocator_policy.has_available_worker()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._allocator_policy.get_worker_by_task_id(task_id)

    def get_worker_ids(self) -> Set[WorkerID]:
        return self._allocator_policy.get_worker_ids()

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            dead_worker
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > self._timeout_seconds
        ]
        for dead_worker in dead_workers:
            await self.__disconnect_worker(dead_worker)

    async def __reroute_tasks(self, task_ids: List[TaskID]):
        for task_id in task_ids:
            await self._task_controller.on_task_reroute(task_id)

    async def __disconnect_worker(self, worker_id: WorkerID):
        """return True if disconnect worker success"""
        if worker_id not in self._worker_alive_since:
            return

        logging.info(f"{worker_id!r} disconnected")
        await self._binder_monitor.send(StateWorker.new_msg(worker_id, b"disconnected"))
        self._worker_alive_since.pop(worker_id)

        task_ids = self._allocator_policy.remove_worker(worker_id)
        if not task_ids:
            return

        logging.info(f"rerouting {len(task_ids)} tasks")
        await self.__reroute_tasks(task_ids)

    async def __shutdown_worker(self, worker_id: WorkerID):
        await self._binder.send(worker_id, ClientDisconnect.new_msg(ClientDisconnect.DisconnectType.Shutdown))
        await self.__disconnect_worker(worker_id)
