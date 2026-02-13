import logging
import time
from typing import Dict, Optional, Set, Tuple

from scaler.io.mixins import AsyncBinder, AsyncConnector
from scaler.protocol.python.common import WorkerState
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    StateWorker,
    Task,
    TaskCancel,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.status import ProcessorStatus, Resource, WorkerManagerStatus, WorkerStatus
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import TaskController, WorkerController
from scaler.scheduler.controllers.policies.mixins import ScalerPolicy
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter

UINT8_MAX = 2**8 - 1


class VanillaWorkerController(WorkerController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, scaler_policy: ScalerPolicy):
        self._config_controller = config_controller

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None
        self._task_controller: Optional[TaskController] = None

        self._worker_alive_since: Dict[WorkerID, Tuple[float, WorkerHeartbeat]] = dict()
        self._scaler_policy = scaler_policy

    def register(self, binder: AsyncBinder, binder_monitor: AsyncConnector, task_controller: TaskController):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._task_controller = task_controller

    def acquire_worker(self, task: Task) -> WorkerID:
        return self._scaler_policy.assign_task(task)

    async def on_task_cancel(self, task_cancel: TaskCancel):
        worker = self._scaler_policy.remove_task(task_cancel.task_id)
        if not worker.is_valid():
            logging.error(f"cannot find task_id={task_cancel.task_id.hex()} in task workers")
            return

        await self._binder.send(worker, task_cancel)

    async def on_task_done(self, task_id: TaskID) -> WorkerID:
        worker = self._scaler_policy.remove_task(task_id)
        if not worker.is_valid():
            logging.error(f"Cannot find task in worker queue: task_id={task_id.hex()}")

        return worker

    async def on_heartbeat(self, worker_id: WorkerID, info: WorkerHeartbeat):
        if self._scaler_policy.add_worker(worker_id, info.capabilities, info.queue_size):
            logging.info(f"worker {worker_id!r} connected")
            await self._binder_monitor.send(StateWorker.new_msg(worker_id, WorkerState.Connected, info.capabilities))
            await self._task_controller.on_worker_connect(worker_id)

        self._worker_alive_since[worker_id] = (time.time(), info)
        await self._binder.send(
            worker_id,
            WorkerHeartbeatEcho.new_msg(
                object_storage_address=self._config_controller.get_config("object_storage_address")
            ),
        )

    async def on_client_shutdown(self, client_id: ClientID):
        for worker in self._scaler_policy.get_worker_ids():
            await self.__shutdown_worker(worker)

    async def on_disconnect(self, worker_id: WorkerID, request: DisconnectRequest):
        await self.__disconnect_worker(request.worker)
        await self._binder.send(worker_id, DisconnectResponse.new_msg(request.worker))

    async def on_disconnect_notification(self, worker_id: WorkerID, notification: WorkerDisconnectNotification):
        await self.__disconnect_worker(notification.worker)

    async def routine(self):
        await self.__clean_workers()

    def get_status(self) -> WorkerManagerStatus:
        worker_to_task_numbers = self._scaler_policy.statistics()
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
        return self._scaler_policy.has_available_worker()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._scaler_policy.get_worker_by_task_id(task_id)

    def get_worker_ids(self) -> Set[WorkerID]:
        return self._scaler_policy.get_worker_ids()

    async def __clean_workers(self):
        now = time.time()
        dead_workers = [
            dead_worker
            for dead_worker, (alive_since, info) in self._worker_alive_since.items()
            if now - alive_since > self._config_controller.get_config("worker_timeout_seconds")
        ]
        for dead_worker in dead_workers:
            await self.__disconnect_worker(dead_worker)

    async def __disconnect_worker(self, worker_id: WorkerID):
        """return True if disconnect worker success"""
        if worker_id not in self._worker_alive_since:
            return

        logging.info(f"{worker_id!r} disconnected")
        await self._binder_monitor.send(StateWorker.new_msg(worker_id, WorkerState.Disconnected, {}))
        self._worker_alive_since.pop(worker_id)

        task_ids = self._scaler_policy.remove_worker(worker_id)
        if not task_ids:
            return

        logging.info(f"{len(task_ids)} task(s) failed due to worker {worker_id!r} disconnected")
        for task_id in task_ids:
            await self._task_controller.on_worker_disconnect(task_id, worker_id)

    async def __shutdown_worker(self, worker_id: WorkerID):
        await self._binder.send(worker_id, ClientDisconnect.new_msg(ClientDisconnect.DisconnectType.Shutdown))
        await self.__disconnect_worker(worker_id)
