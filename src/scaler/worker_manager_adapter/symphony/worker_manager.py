import asyncio
import logging
import os
import signal
import uuid
from typing import Dict, List, Tuple

from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.worker_manager_adapter.symphony.worker import SymphonyWorker

Status = WorkerManagerCommandResponse.Status


class SymphonyWorkerManager:
    def __init__(self, config: SymphonyWorkerManagerConfig):
        self._address = config.worker_manager_config.scheduler_address
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._service_name = config.service_name
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_config.io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._event_loop = config.worker_config.event_loop

        self._backend: NetworkBackend = get_network_backend_from_env(io_threads=self._io_threads)
        self._name = "worker_manager_symphony"
        self._ident = generate_identity_from_name(self._name)

        self._connector_external: AsyncConnector = self._backend.create_async_connector(
            identity=self._ident, callback=self.__on_receive_external
        )

        self._workers: Dict[WorkerID, SymphonyWorker] = {}

    async def __on_receive_external(self, message: BaseMessage):
        if isinstance(message, WorkerManagerCommand):
            await self._handle_command(message)

        elif isinstance(message, WorkerManagerHeartbeatEcho):
            pass

        else:
            logging.warning(f"Received unknown message type: {type(message)}")

    async def _handle_command(self, command: WorkerManagerCommand):
        cmd_type = command.command
        response_status: Status = Status.success
        worker_ids: List[bytes] = []

        if cmd_type == WorkerManagerCommandType.startWorkers:
            new_wid, response_status = await self.start_worker()
            if response_status == Status.success:
                worker_ids = [bytes(new_wid)]
        elif cmd_type == WorkerManagerCommandType.shutdownWorkers:
            response_status = await self.shutdown_workers(command.workerIDs)
            if response_status == Status.success:
                worker_ids = list(command.workerIDs)
        else:
            raise ValueError("Unknown WorkerManagerCommand")

        await self._connector_external.send(
            WorkerManagerCommandResponse(
                command=cmd_type, status=response_status, workerIDs=worker_ids, capabilities=self._capabilities
            )
        )

    async def start_worker(self) -> Tuple[WorkerID, Status]:
        if len(self._workers) >= self._max_task_concurrency != -1:
            return WorkerID(b""), Status.tooManyWorkers

        worker = SymphonyWorker(
            name=f"SYM|{uuid.uuid4().hex}",
            address=self._worker_scheduler_address,
            object_storage_address=self._object_storage_address,
            service_name=self._service_name,
            base_concurrency=self._max_task_concurrency,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            event_loop=self._event_loop,
            worker_manager_id=self._worker_manager_id,
        )

        worker.start()
        self._workers[worker.identity] = worker
        return worker.identity, Status.success

    async def shutdown_workers(self, worker_ids: List[bytes]) -> Status:
        if not worker_ids:
            return Status.workerNotFound

        for wid_bytes in worker_ids:
            wid = WorkerID(wid_bytes)
            if wid not in self._workers:
                logging.warning(f"Worker with ID {wid!r} does not exist.")
                return Status.workerNotFound

        for wid_bytes in worker_ids:
            wid = WorkerID(wid_bytes)
            worker = self._workers.pop(wid)
            os.kill(worker.pid, signal.SIGINT)
            worker.join()

        return Status.success

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    def _cleanup(self):
        if self._connector_external is not None:
            self._connector_external.destroy()

    def __destroy(self):
        print(f"Worker manager {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

    async def _run(self) -> None:
        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

    async def __send_heartbeat(self):
        await self._connector_external.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=self._max_task_concurrency,
                capabilities=self._capabilities,
                workerManagerID=self._worker_manager_id,
            )
        )

    async def __get_loops(self):
        await self._connector_external.connect(self._address, ConnectorRemoteType.Binder)

        loops = [
            create_async_loop_routine(self._connector_external.routine, 0),
            create_async_loop_routine(self.__send_heartbeat, self._heartbeat_interval_seconds),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ymq.YMQException as e:
            if e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                pass
            else:
                logging.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")
