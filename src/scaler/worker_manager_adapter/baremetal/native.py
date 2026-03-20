import asyncio
import logging
import os
import signal
import uuid
from typing import Any, Dict, List, Optional, Tuple

import zmq

from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector
from scaler.io.utility import create_async_connector, create_async_simple_context
from scaler.protocol.python.message import (
    Message,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.worker import Worker

Status = WorkerManagerCommandResponse.Status


class NativeWorkerManager:
    def __init__(self, config: NativeWorkerManagerConfig):
        self._address = config.worker_manager_config.scheduler_address
        self._object_storage_address = config.worker_manager_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._worker_manager_id = config.worker_manager_id.encode()
        self._io_threads = config.worker_io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._task_timeout_seconds = config.worker_config.task_timeout_seconds
        self._death_timeout_seconds = config.worker_config.death_timeout_seconds
        self._garbage_collect_interval_seconds = config.worker_config.garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = config.worker_config.trim_memory_threshold_bytes
        self._hard_processor_suspend = config.worker_config.hard_processor_suspend
        self._event_loop = config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file
        self._preload = config.preload
        self._mode = config.mode

        if config.worker_type is not None:
            self._worker_prefix = config.worker_type
        elif self._mode == NativeWorkerManagerMode.FIXED:
            self._worker_prefix = "FIX"
        elif self._mode == NativeWorkerManagerMode.DYNAMIC:
            self._worker_prefix = "NAT"
        else:
            raise ValueError(f"worker_type is not set and mode is unrecognised: {self._mode!r}")

        self._workers: Dict[WorkerID, Worker] = {}

        # ZMQ setup is deferred to _setup_zmq(), called at the start of run().
        # This keeps the object picklable so callers can do Process(target=adapter.run).start().
        self._context: Optional[Any] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._ident: Optional[bytes] = None

    def _setup_zmq(self) -> None:
        self._name = "worker_manager_native"

        self._ident = f"{self._name}|{uuid.uuid4().bytes.hex()}".encode()
        self._context = create_async_simple_context()
        self._connector_external = create_async_connector(
            self._context,
            name="worker_manager_native",
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

    def _create_worker(self) -> Worker:
        return Worker(
            name=f"{self._worker_prefix}|{uuid.uuid4().hex}",
            address=self._address,
            object_storage_address=self._object_storage_address,
            preload=self._preload,
            capabilities=self._capabilities,
            io_threads=self._io_threads,
            task_queue_size=self._task_queue_size,
            heartbeat_interval_seconds=self._heartbeat_interval_seconds,
            task_timeout_seconds=self._task_timeout_seconds,
            death_timeout_seconds=self._death_timeout_seconds,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            event_loop=self._event_loop,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
            worker_manager_id=self._worker_manager_id,
        )

    def _spawn_initial_workers(self) -> None:
        for _ in range(self._max_task_concurrency):
            worker = self._create_worker()
            worker.start()
            self._workers[worker.identity] = worker

    async def __on_receive_external(self, message: Message):
        if isinstance(message, WorkerManagerCommand):
            await self._handle_command(message)

        elif isinstance(message, WorkerManagerHeartbeatEcho):
            pass

        else:
            print(f"Received unknown message type: {type(message)}")

    async def _handle_command(self, command: WorkerManagerCommand):
        cmd_type = command.command
        response_status = Status.Success
        worker_ids: List[bytes] = []

        if cmd_type == WorkerManagerCommandType.StartWorkers:
            new_wid, response_status = await self.start_worker()
            if response_status == Status.Success:
                worker_ids = [bytes(new_wid)]
        elif cmd_type == WorkerManagerCommandType.ShutdownWorkers:
            response_status = await self.shutdown_workers(command.worker_ids)
            if response_status == Status.Success:
                worker_ids = command.worker_ids
        else:
            raise ValueError("Unknown WorkerManagerCommand")

        await self._connector_external.send(
            WorkerManagerCommandResponse.new_msg(
                command=cmd_type, status=response_status, worker_ids=worker_ids, capabilities=self._capabilities
            )
        )

    async def start_worker(self) -> Tuple[WorkerID, Status]:
        if len(self._workers) >= self._max_task_concurrency != -1:
            return WorkerID(b""), Status.TooManyWorkers

        worker = self._create_worker()
        worker.start()
        self._workers[worker.identity] = worker
        logging.info(f"Start worker, {self._ident!r}")
        return worker.identity, Status.Success

    async def shutdown_workers(self, worker_ids: List[bytes]) -> Status:
        if not worker_ids:
            return Status.WorkerNotFound

        for wid_bytes in worker_ids:
            wid = WorkerID(wid_bytes)
            if wid not in self._workers:
                logging.warning(f"Worker with ID {wid!r} does not exist.")
                return Status.WorkerNotFound

        for wid_bytes in worker_ids:
            wid = WorkerID(wid_bytes)
            worker = self._workers.pop(wid)
            os.kill(worker.pid, signal.SIGINT)
            worker.join()

        return Status.Success

    def run(self) -> None:
        if self._mode == NativeWorkerManagerMode.FIXED:
            self._run_fixed()
            return

        # DYNAMIC mode
        self._setup_zmq()
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    def _run_fixed(self) -> None:
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        register_event_loop(self._event_loop)
        self._spawn_initial_workers()

        def _on_signal(sig: int, frame: object) -> None:
            logging.info("NativeWorkerManager (FIXED): received signal %d, terminating workers", sig)
            for worker in self._workers.values():
                if worker.is_alive():
                    worker.terminate()

        signal.signal(signal.SIGTERM, _on_signal)
        signal.signal(signal.SIGINT, _on_signal)

        for worker in self._workers.values():
            worker.join()

    def _cleanup(self) -> None:
        if self._connector_external is not None:
            self._connector_external.destroy()

    def __destroy(self):
        print(f"Worker manager {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

    async def _run(self) -> None:
        register_event_loop(self._event_loop)
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)

        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

    async def __send_heartbeat(self) -> None:
        await self._connector_external.send(
            WorkerManagerHeartbeat.new_msg(
                max_task_concurrency=self._max_task_concurrency,
                capabilities=self._capabilities,
                worker_manager_id=self._worker_manager_id,
            )
        )

    async def __get_loops(self) -> None:
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
