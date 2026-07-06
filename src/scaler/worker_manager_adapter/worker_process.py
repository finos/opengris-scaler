import asyncio
import logging
import multiprocessing
from collections import deque
from typing import Callable, Dict, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.protocol.capnp import (
    BaseMessage,
    ClientDisconnect,
    ObjectInstruction,
    Task,
    TaskCancel,
    WorkerDisconnectNotification,
    WorkerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.signal_handler import install_async_shutdown_handler
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker_manager_adapter.heartbeat_manager import HeartbeatManager
from scaler.worker_manager_adapter.mixins import ExecutionBackend, ProcessorStatusProvider
from scaler.worker_manager_adapter.task_manager import TaskManager

logger = logging.getLogger(__name__)

_SpawnProcess = multiprocessing.get_context("spawn").Process


class WorkerProcess(_SpawnProcess):  # type: ignore[valid-type, misc]
    def __init__(
        self,
        name: str,
        address: AddressConfig,
        object_storage_address: Optional[AddressConfig],
        capabilities: Dict[str, int],
        base_concurrency: int,
        heartbeat_interval_seconds: int,
        death_timeout_seconds: int,
        task_queue_size: int,
        io_threads: int,
        event_loop: str,
        worker_manager_id: bytes,
        processor_status_provider_factory: Callable[[], ProcessorStatusProvider],
        execution_backend_factory: Callable[[], ExecutionBackend],
        idle_sleep_seconds: float = 0.0,
        security_config: Optional[SecurityConfig] = None,
    ) -> None:
        super().__init__(name=name)

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._object_storage_address = object_storage_address
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._security_config = security_config

        self._ident = WorkerID.generate_worker_id(name)

        self._base_concurrency = base_concurrency

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._task_queue_size = task_queue_size
        self._worker_manager_id = worker_manager_id

        self._processor_status_provider_factory = processor_status_provider_factory
        self._execution_backend_factory = execution_backend_factory
        self._idle_sleep_seconds = idle_sleep_seconds

        self._backend: Optional[NetworkBackend] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._execution_backend: Optional[ExecutionBackend] = None
        self._task_manager: Optional[TaskManager] = None
        self._heartbeat_manager: Optional[HeartbeatManager] = None

        self._heartbeat_received: bool = False
        self._backoff_message_queue: deque = deque()

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

    def _cleanup(self) -> None:
        if self._connector_external is not None:
            self._connector_external.destroy()

        if self._connector_storage is not None:
            self._connector_storage.destroy()

    def __initialize(self) -> None:
        setup_logger()
        register_event_loop(self._event_loop)

        self._backend = get_network_backend_from_env(io_threads=self._io_threads)

        self._connector_external = self._backend.create_async_connector(
            identity=self._ident, callback=self.__on_receive_external
        )

        self._connector_storage = self._backend.create_async_object_storage_connector(identity=self._ident)

        self._execution_backend = self._execution_backend_factory()
        processor_status_provider = self._processor_status_provider_factory()

        self._heartbeat_manager = HeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
            worker_manager_id=self._worker_manager_id,
            processor_status_provider=processor_status_provider,
            security_config=self._security_config,
        )
        self._task_manager = TaskManager(
            base_concurrency=self._base_concurrency,
            execution_backend=self._execution_backend,
            idle_sleep_seconds=self._idle_sleep_seconds,
        )
        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)

        self._heartbeat_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            worker_task_manager=self._task_manager,
            timeout_manager=self._timeout_manager,
        )
        self._task_manager.register(
            connector_external=self._connector_external,
            connector_storage=self._connector_storage,
            heartbeat_manager=self._heartbeat_manager,
        )

    async def __on_receive_external(self, message: BaseMessage) -> None:
        if not self._heartbeat_received and not isinstance(message, WorkerHeartbeatEcho):
            self._backoff_message_queue.append(message)
            return

        if isinstance(message, WorkerHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            self._heartbeat_received = True

            while self._backoff_message_queue:
                backoff_message = self._backoff_message_queue.popleft()
                await self.__on_receive_external(backoff_message)

            return

        if isinstance(message, Task):
            await self._task_manager.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, ObjectInstruction):
            await self._task_manager.on_object_instruction(message)
            return

        if isinstance(message, ClientDisconnect):
            if message.disconnectType == ClientDisconnect.DisconnectType.shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logger.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self) -> None:
        await self._connector_external.connect(
            self._address, ConnectorRemoteType.Binder, security_config=self._security_config
        )

        if self._object_storage_address is not None:
            await self._connector_storage.connect(self._object_storage_address, security_config=self._security_config)

        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._execution_backend.routine, 0),
                create_async_loop_routine(self._task_manager.process_task, 0),
                create_async_loop_routine(self._task_manager.resolve_tasks, 0),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logger.info(f"{self.identity!r}: {str(e)}")
        except Exception as e:
            logger.exception(f"{self.identity!r}: failed with unhandled exception:\n{e}")

        try:
            try:
                await self._connector_external.send(WorkerDisconnectNotification(worker=self.identity))
            except ymq.YMQException as e:
                if e.code != ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                    raise
        finally:
            logger.info(f"{self.identity!r}: quit")

    def __register_signal(self) -> None:
        install_async_shutdown_handler(self._loop, self.__destroy)

    def __destroy(self) -> None:
        self._task.cancel()
