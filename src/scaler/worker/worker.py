import asyncio
import logging
import multiprocessing
import os
import signal
import tempfile
import uuid
from typing import Dict, Optional, Tuple

import zmq.asyncio

from scaler.config.defaults import PROFILING_INTERVAL_SECONDS
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig, ZMQType
from scaler.io.async_binder import ZMQAsyncBinder
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.mixins import AsyncBinder, AsyncConnector, AsyncObjectStorageConnector
from scaler.io.utility import create_async_object_storage_connector
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    ObjectInstruction,
    ProcessorInitialized,
    Task,
    TaskCancel,
    TaskLog,
    TaskResult,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ProcessorID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.heartbeat_manager import VanillaHeartbeatManager
from scaler.worker.agent.processor_manager import VanillaProcessorManager
from scaler.worker.agent.profiling_manager import VanillaProfilingManager
from scaler.worker.agent.task_manager import VanillaTaskManager
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager

try:
    from scaler.io.ymq import ymq
except ImportError:
    ymq = None


class Worker(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        name: str,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageConfig],
        preload: Optional[str],
        capabilities: Dict[str, int],
        io_threads: int,
        task_queue_size: int,
        heartbeat_interval_seconds: int,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        task_timeout_seconds: int,
        death_timeout_seconds: int,
        hard_processor_suspend: bool,
        logging_paths: Tuple[str, ...],
        logging_level: str,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._object_storage_address = object_storage_address
        self._preload = preload
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._task_queue_size = task_queue_size

        self._ident = WorkerID.generate_worker_id(name)  # _identity is internal to multiprocessing.Process

        self._address_path_internal = os.path.join(tempfile.gettempdir(), f"scaler_worker_{uuid.uuid4().hex}")
        self._address_internal = ZMQConfig(ZMQType.ipc, host=self._address_path_internal)

        self._task_queue_size = task_queue_size
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._task_timeout_seconds = task_timeout_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._hard_processor_suspend = hard_processor_suspend

        self._logging_paths = logging_paths
        self._logging_level = logging_level

        self._context: Optional[zmq.asyncio.Context] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._binder_internal: Optional[AsyncBinder] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager: Optional[VanillaTaskManager] = None
        self._heartbeat_manager: Optional[VanillaHeartbeatManager] = None
        self._profiling_manager: Optional[VanillaProfilingManager] = None
        self._processor_manager: Optional[VanillaProcessorManager] = None

    @property
    def identity(self) -> WorkerID:
        return self._ident

    def run(self) -> None:
        self.__initialize()
        self.__run_forever()

    def __initialize(self):
        setup_logger()
        register_event_loop(self._event_loop)

        self._context = zmq.asyncio.Context()
        self._connector_external = ZMQAsyncConnector(
            context=self._context,
            name=self.name,
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

        self._binder_internal = ZMQAsyncBinder(
            context=self._context, name=self.name, address=self._address_internal, identity=self._ident
        )
        self._binder_internal.register(self.__on_receive_internal)

        self._connector_storage = create_async_object_storage_connector()

        self._heartbeat_manager = VanillaHeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
        )

        self._profiling_manager = VanillaProfilingManager()
        self._task_manager = VanillaTaskManager(task_timeout_seconds=self._task_timeout_seconds)
        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)
        self._processor_manager = VanillaProcessorManager(
            identity=self._ident,
            event_loop=self._event_loop,
            address_internal=self._address_internal,
            scheduler_address=self._address,
            preload=self._preload,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
        )

        # register
        self._task_manager.register(connector=self._connector_external, processor_manager=self._processor_manager)
        self._heartbeat_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            worker_task_manager=self._task_manager,
            timeout_manager=self._timeout_manager,
            processor_manager=self._processor_manager,
        )
        self._processor_manager.register(
            heartbeat_manager=self._heartbeat_manager,
            task_manager=self._task_manager,
            profiling_manager=self._profiling_manager,
            connector_external=self._connector_external,
            binder_internal=self._binder_internal,
            connector_storage=self._connector_storage,
        )

        self._loop = asyncio.get_event_loop()
        self.__register_signal()
        self._task = self._loop.create_task(self.__get_loops())

    async def __on_receive_external(self, message: Message):
        if isinstance(message, WorkerHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            return

        if isinstance(message, Task):
            await self._task_manager.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, ObjectInstruction):
            await self._processor_manager.on_external_object_instruction(message)
            return

        if isinstance(message, ClientDisconnect):
            if message.disconnect_type == ClientDisconnect.DisconnectType.Shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        raise TypeError(f"Unknown {message=}")

    async def __on_receive_internal(self, processor_id_bytes: bytes, message: Message):
        processor_id = ProcessorID(processor_id_bytes)

        if isinstance(message, ProcessorInitialized):
            await self._processor_manager.on_processor_initialized(processor_id, message)
            return

        if isinstance(message, ObjectInstruction):
            await self._processor_manager.on_internal_object_instruction(processor_id, message)
            return

        if isinstance(message, TaskLog):
            await self._connector_external.send(message)
            return

        if isinstance(message, TaskResult):
            await self._processor_manager.on_task_result(processor_id, message)
            return

        raise TypeError(f"Unknown message from {processor_id!r}: {message}")

    async def __get_loops(self):
        if self._object_storage_address is not None:
            # With a manually set storage address, immediately connect to the object storage server.
            await self._connector_storage.connect(self._object_storage_address.host, self._object_storage_address.port)

        try:
            await asyncio.gather(
                self._processor_manager.initialize(),
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._binder_internal.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._profiling_manager.routine, PROFILING_INTERVAL_SECONDS),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"{self.identity!r}: {str(e)}")
        except Exception as e:
            # TODO: Should the object storage connector catch this error?
            if (
                ymq is not None
                and isinstance(e, ymq.YMQException)
                and e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd
            ):
                pass
            else:
                logging.exception(f"{self.identity!r}: failed with unhandled exception:\n{e}")

        await self._connector_external.send(DisconnectRequest.new_msg(self.identity))

        self._connector_external.destroy()
        self._processor_manager.destroy("quit")
        self._binder_internal.destroy()
        os.remove(self._address_path_internal)

        logging.info(f"{self.identity!r}: quit")

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)

    def __destroy(self):
        self._task.cancel()
