import logging
import time
from typing import Dict, List, Optional, Set, Tuple

from scaler.io.mixins import AsyncBinder, AsyncPublisher
from scaler.protocol.capnp import (
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    ObjectStorageAddress,
    ProcessorStatus,
    Resource,
    StateWorker,
    Task,
    TaskCancel,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
    WorkerManagerStatus,
    WorkerState,
    WorkerStatus,
)
from scaler.protocol.helpers import capabilities_to_dict
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.mixins import PolicyController, TaskController, WorkerController
from scaler.utility.identifiers import ClientID, TaskID, WorkerID
from scaler.utility.mixins import Looper, Reporter

UINT8_MAX = 2**8 - 1


class VanillaWorkerController(WorkerController, Looper, Reporter):
    def __init__(self, config_controller: VanillaConfigController, policy_controller: PolicyController):
        self._config_controller = config_controller

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncPublisher] = None
        self._task_controller: Optional[TaskController] = None

        self._worker_alive_since: Dict[WorkerID, Tuple[float, WorkerHeartbeat]] = dict()
        self._worker_to_manager: Dict[WorkerID, bytes] = dict()
        self._manager_to_workers: Dict[bytes, Set[WorkerID]] = dict()
        self._policy_controller = policy_controller

    def register(self, binder: AsyncBinder, binder_monitor: AsyncPublisher, task_controller: TaskController):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._task_controller = task_controller

    def acquire_worker(self, task: Task) -> WorkerID:
        return self._policy_controller.assign_task(task)

    async def on_task_cancel(self, task_cancel: TaskCancel) -> WorkerID:
        worker = self._policy_controller.remove_task(task_cancel.taskId)
        if not worker.is_valid():
            logging.error(f"cannot find task_id={task_cancel.taskId.hex()} in task workers")

        return worker

    async def on_task_done(self, task_id: TaskID) -> WorkerID:
        worker = self._policy_controller.remove_task(task_id)
        if not worker.is_valid():
            logging.error(f"Cannot find task in worker queue: task_id={task_id.hex()}")

        return worker

    async def on_heartbeat(self, worker_id: WorkerID, info: WorkerHeartbeat):
        info.capabilities = capabilities_to_dict(info.capabilities)
        if self._policy_controller.add_worker(worker_id, info.capabilities, info.queueSize):
            logging.info(f"worker {worker_id!r} connected")
            await self._binder_monitor.send(
                StateWorker(workerId=worker_id, state=WorkerState.connected, capabilities=info.capabilities)
            )
            await self._task_controller.on_worker_connect(worker_id)

        if worker_id not in self._worker_to_manager:
            self._worker_to_manager[worker_id] = info.workerManagerID
            self._manager_to_workers.setdefault(info.workerManagerID, set()).add(worker_id)

        self._worker_alive_since[worker_id] = (time.time(), info)

        object_storage_address = self._config_controller.get_config("advertised_object_storage_address")
        await self._binder.send(
            worker_id,
            WorkerHeartbeatEcho(
                objectStorageAddress=ObjectStorageAddress(
                    host=object_storage_address.host, port=object_storage_address.port
                )
            ),
        )

    async def on_client_shutdown(self, client_id: ClientID):
        for worker in self._policy_controller.get_worker_ids():
            await self.__shutdown_worker(worker)

    async def on_disconnect(self, worker_id: WorkerID, request: DisconnectRequest):
        await self.__disconnect_worker(request.worker)
        await self._binder.send(worker_id, DisconnectResponse(worker=request.worker))

    async def routine(self):
        await self.__clean_workers()

    def get_status(self) -> WorkerManagerStatus:
        worker_to_task_numbers = self._policy_controller.statistics()
        return WorkerManagerStatus(
            workers=[
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
            debug_info = f"{int(current_processor.initialized)}{int(current_processor.hasTask)}{int(info.taskLock)}"
        else:
            debug_info = f"00{int(info.taskLock)}"

        return WorkerStatus(
            workerId=worker_id,
            agent=info.agent,
            rssFree=info.rssFree,
            free=worker_task_numbers["free"],
            sent=worker_task_numbers["sent"],
            queued=info.queuedTasks,
            suspended=suspended,
            lagUS=info.latencyUS,
            lastS=last_s,
            itl=debug_info,
            processorStatuses=[
                ProcessorStatus(
                    pid=p.pid,
                    initialized=p.initialized,
                    hasTask=p.hasTask,
                    suspended=p.suspended,
                    resource=Resource(cpu=p.resource.cpu, rss=p.resource.rss),
                )
                for p in info.processors
            ],
        )

    def has_available_worker(self) -> bool:
        return self._policy_controller.has_available_worker()

    def get_worker_by_task_id(self, task_id: TaskID) -> WorkerID:
        return self._policy_controller.get_worker_by_task_id(task_id)

    def get_worker_ids(self) -> Set[WorkerID]:
        return self._policy_controller.get_worker_ids()

    def get_workers_by_manager_id(self, manager_id: bytes) -> List[WorkerID]:
        return list(self._manager_to_workers.get(manager_id, set()))

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
        await self._binder_monitor.send(
            StateWorker(workerId=worker_id, state=WorkerState.disconnected, capabilities={})
        )
        self._worker_alive_since.pop(worker_id)
        manager_id = self._worker_to_manager.pop(worker_id)
        workers_set = self._manager_to_workers[manager_id]
        workers_set.discard(worker_id)
        if not workers_set:
            del self._manager_to_workers[manager_id]

        task_ids = self._policy_controller.remove_worker(worker_id)
        if not task_ids:
            return

        logging.info(f"{len(task_ids)} task(s) failed due to worker {worker_id!r} disconnected")
        for task_id in task_ids:
            await self._task_controller.on_worker_disconnect(task_id, worker_id)

    async def __shutdown_worker(self, worker_id: WorkerID):
        await self._binder.send(worker_id, ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown))
        await self.__disconnect_worker(worker_id)
