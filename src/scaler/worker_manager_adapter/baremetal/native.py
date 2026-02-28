import asyncio
import logging
import os
import signal
import uuid
from typing import Dict, Tuple

import zmq

from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.io.utility import create_async_connector, create_async_simple_context
from scaler.io.ymq import ymq
from scaler.protocol.python.message import (
    Message,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
    WorkerAdapterHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.worker import Worker
from scaler.worker_manager_adapter.common import WorkerGroupID

Status = WorkerAdapterCommandResponse.Status


class NativeWorkerAdapter:
    def __init__(self, config: NativeWorkerAdapterConfig):
        self._address = config.worker_adapter_config.scheduler_address
        self._object_storage_address = config.worker_adapter_config.object_storage_address
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._io_threads = config.worker_io_threads
        self._task_queue_size = config.worker_config.per_worker_task_queue_size
        self._max_workers = config.worker_adapter_config.max_workers
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
        self._workers_per_group = 1

        self._context = create_async_simple_context()
        self._name = "worker_adapter_native"

        self._ident = f"{self._name}|{uuid.uuid4().bytes.hex()}".encode()
        self._connector_external = create_async_connector(
            self._context,
            name="worker_adapter_native",
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

        """
        Although a worker group can contain multiple workers, in this native adapter implementation,
        each worker group will only contain one worker.
        """
        self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, Worker]] = {}

    async def __on_receive_external(self, message: Message):
        if isinstance(message, WorkerAdapterCommand):
            await self._handle_command(message)

        elif isinstance(message, WorkerAdapterHeartbeatEcho):
            pass

        else:
            print(f"Received unknown message type: {type(message)}")

    async def _handle_command(self, command: WorkerAdapterCommand):
        cmd_type = command.command
        worker_group_id = command.worker_group_id
        response_status = Status.Success

        cmd_res = WorkerAdapterCommandType.StartWorkerGroup
        if cmd_type == WorkerAdapterCommandType.StartWorkerGroup:
            cmd_res = WorkerAdapterCommandType.StartWorkerGroup
            worker_group_id, response_status = await self.start_worker_group()
        elif cmd_type == WorkerAdapterCommandType.ShutdownWorkerGroup:
            cmd_res = WorkerAdapterCommandType.ShutdownWorkerGroup
            response_status = await self.shutdown_worker_group(worker_group_id)
        else:
            raise ValueError("Unknown WorkerAdapterCommand")

        await self._connector_external.send(
            WorkerAdapterCommandResponse.new_msg(
                worker_group_id=worker_group_id, command=cmd_res, status=response_status
            )
        )
        return

    async def start_worker_group(self) -> Tuple[WorkerGroupID, Status]:
        num_of_workers = sum(len(workers) for workers in self._worker_groups.values())
        if num_of_workers >= self._max_workers != -1:
            return b"", Status.WorkerGroupTooMuch

        worker = Worker(
            name=f"NAT|{uuid.uuid4().hex}",
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
        )

        worker.start()
        worker_group_id = f"native-{uuid.uuid4().hex}".encode()
        self._worker_groups[worker_group_id] = {worker.identity: worker}
        print(f"Start worker group, {self._ident!r}")
        return worker_group_id, Status.Success

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> Status:
        if not worker_group_id:
            return Status.WorkerGroupIDNotSpecified

        if worker_group_id not in self._worker_groups:
            logging.warning(f"Worker group with ID {bytes(worker_group_id).decode()} does not exist.")
            return Status.WorkerGroupIDNotFound

        for worker in self._worker_groups[worker_group_id].values():
            os.kill(worker.pid, signal.SIGINT)
            worker.join()

        self._worker_groups.pop(worker_group_id)
        return Status.Success

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    def _cleanup(self):
        if self._connector_external is not None:
            self._connector_external.destroy()

    def __destroy(self):
        print(f"Worker adapter {self._ident!r} received signal, shutting down")
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

    async def __send_heartbeat(self):
        await self._connector_external.send(
            WorkerAdapterHeartbeat.new_msg(
                max_worker_groups=self._max_workers,
                workers_per_group=self._workers_per_group,
                capabilities=self._capabilities,
            )
        )

    async def __get_loops(self):
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
