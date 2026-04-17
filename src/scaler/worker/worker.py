import asyncio
import logging
import multiprocessing
import pathlib
import signal
import uuid
from typing import Dict, Optional, Tuple

from scaler.config.defaults import PROFILING_INTERVAL_SECONDS
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io import ymq
from scaler.io.mixins import (
    AsyncBinder,
    AsyncConnector,
    AsyncObjectStorageConnector,
    ConnectorRemoteType,
    NetworkBackend,
)
from scaler.io.network_backends import YMQNetworkBackend, ZMQNetworkBackend, get_network_backend_from_env
from scaler.protocol.capnp import (
    BaseMessage,
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    ObjectInstruction,
    ProcessorInitialized,
    Task,
    TaskCancel,
    TaskLog,
    TaskResult,
    WorkerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.exceptions import ClientShutdownException, ObjectStorageException
from scaler.utility.identifiers import ProcessorID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.heartbeat_manager import VanillaHeartbeatManager
from scaler.worker.agent.processor_manager import VanillaProcessorManager
from scaler.worker.agent.profiling_manager import VanillaProfilingManager
from scaler.worker.agent.task_manager import VanillaTaskManager
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager


class Worker(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        name: str,
        address: AddressConfig,
        object_storage_address: Optional[AddressConfig],
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
        worker_manager_id: bytes,
        deterministic_worker_ids: bool = False,
    ):
        super().__init__(name="Agent")

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._object_storage_address = object_storage_address
        self._preload = preload
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._task_queue_size = task_queue_size

        if deterministic_worker_ids:
            self._ident = WorkerID(name.encode())
        else:
            self._ident = WorkerID.generate_worker_id(name)

        self._address_internal: Optional[AddressConfig] = None

        self._task_queue_size = task_queue_size
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._garbage_collect_interval_seconds = garbage_collect_interval_seconds
        self._trim_memory_threshold_bytes = trim_memory_threshold_bytes
        self._task_timeout_seconds = task_timeout_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._hard_processor_suspend = hard_processor_suspend

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._worker_manager_id = worker_manager_id

        self._backend: Optional[NetworkBackend] = None
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
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    async def _run(self) -> None:
        self.__initialize()

        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

    def _cleanup(self):
        # the storage connector has asyncio resources that need to be cleaned up
        # before the event loop is closed
        if self._connector_storage is not None:
            self._connector_storage.destroy()

    def __initialize(self):
        setup_logger()
        register_event_loop(self._event_loop)

        self._backend = get_network_backend_from_env(io_threads=self._io_threads)

        self._address_internal = self._backend.create_internal_address(
            f"scaler_worker_{uuid.uuid4().hex}", same_process=False
        )

        self._connector_external = self._backend.create_async_connector(
            identity=self._ident, callback=self.__on_receive_external
        )

        self._binder_internal = self._backend.create_async_binder(
            identity=self._ident, callback=self.__on_receive_internal
        )

        self._connector_storage = self._backend.create_async_object_storage_connector(identity=self._ident)

        self._heartbeat_manager = VanillaHeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
            worker_manager_id=self._worker_manager_id,
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

    async def __on_receive_external(self, message: BaseMessage):
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
            if message.disconnectType == ClientDisconnect.DisconnectType.shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        if isinstance(message, DisconnectResponse):
            logging.error("Worker initiated DisconnectRequest got replied")
            self._task.cancel()
            return

        raise TypeError(f"Unknown {message=}")

    async def __on_receive_internal(self, processor_id_bytes: bytes, message: BaseMessage):
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
        await self._connector_external.connect(self._address, ConnectorRemoteType.Binder)
        await self._binder_internal.bind(self._address_internal)

        if self._object_storage_address is not None:
            # With a manually set storage address, immediately connect to the object storage server.
            await self._connector_storage.connect(self._object_storage_address)

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

        except ObjectStorageException:
            pass

        # TODO: Should the object storage connector catch this error?
        except ymq.YMQException as e:
            if e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                pass
            else:
                logging.exception(f"{self.identity!r}: failed with unhandled exception:\n{e}")
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"{self.identity!r}: {str(e)}")
        except Exception as e:
            logging.exception(f"{self.identity!r}: failed with unhandled exception:\n{e}")

        if isinstance(self._backend, ZMQNetworkBackend):
            await self.__graceful_shutdown()

        self._connector_external.destroy()
        self._processor_manager.destroy("quit")
        self._binder_internal.destroy()
        self._connector_storage.destroy()

        if self._address_internal.type == SocketType.ipc:
            pathlib.Path(self._address_internal.host).unlink(missing_ok=True)

        logging.info(f"{self.identity!r}: quit")

    def __register_signal(self):
        if isinstance(self._backend, ZMQNetworkBackend):
            self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
            self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)
        elif isinstance(self._backend, YMQNetworkBackend):
            self._loop.add_signal_handler(signal.SIGINT, lambda: asyncio.ensure_future(self.__graceful_shutdown()))
            self._loop.add_signal_handler(signal.SIGTERM, lambda: asyncio.ensure_future(self.__graceful_shutdown()))

    async def __graceful_shutdown(self):
        try:
            await self._connector_external.send(DisconnectRequest(worker=self.identity))
        except ymq.YMQException:
            pass

    def __destroy(self):
        self._task.cancel()
