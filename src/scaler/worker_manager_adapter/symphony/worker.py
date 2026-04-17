import asyncio
import logging
import multiprocessing
import signal
from collections import deque
from typing import Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import YMQNetworkBackend, ZMQNetworkBackend, get_network_backend_from_env
from scaler.protocol.capnp import (
    BaseMessage,
    ClientDisconnect,
    DisconnectRequest,
    DisconnectResponse,
    ObjectInstruction,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker_manager_adapter.symphony.heartbeat_manager import SymphonyHeartbeatManager
from scaler.worker_manager_adapter.symphony.task_manager import SymphonyTaskManager


class SymphonyWorker(multiprocessing.get_context("spawn").Process):  # type: ignore
    """
    SymphonyWorker is an implementation of a worker that can handle multiple tasks concurrently.
    Most of the task execution logic is handled by SymphonyTaskManager.
    """

    def __init__(
        self,
        name: str,
        address: AddressConfig,
        object_storage_address: Optional[AddressConfig],
        service_name: str,
        capabilities: Dict[str, int],
        base_concurrency: int,
        heartbeat_interval_seconds: int,
        death_timeout_seconds: int,
        task_queue_size: int,
        io_threads: int,
        event_loop: str,
        worker_manager_id: bytes,
    ):
        super().__init__(name="Agent")

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._object_storage_address = object_storage_address
        self._capabilities = capabilities
        self._io_threads = io_threads

        self._ident = WorkerID.generate_worker_id(name)  # _identity is internal to multiprocessing.Process

        self._service_name = service_name
        self._base_concurrency = base_concurrency

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._task_queue_size = task_queue_size
        self._worker_manager_id = worker_manager_id

        self._backend: Optional[NetworkBackend] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager: Optional[SymphonyTaskManager] = None
        self._heartbeat_manager: Optional[SymphonyHeartbeatManager] = None

        """
        Sometimes the first message received is not a heartbeat echo, so we need to backoff processing other tasks
        until we receive the first heartbeat echo.
        """
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

    def _cleanup(self):
        if self._connector_storage is not None:
            self._connector_storage.destroy()

    def __initialize(self):
        setup_logger()
        register_event_loop(self._event_loop)

        self._backend = get_network_backend_from_env(io_threads=self._io_threads)

        self._connector_external = self._backend.create_async_connector(
            identity=self._ident, callback=self.__on_receive_external
        )

        self._connector_storage = self._backend.create_async_object_storage_connector(identity=self._ident)

        self._heartbeat_manager = SymphonyHeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
            worker_manager_id=self._worker_manager_id,
        )
        self._task_manager = SymphonyTaskManager(
            base_concurrency=self._base_concurrency, service_name=self._service_name
        )
        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)

        # register
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

    async def __on_receive_external(self, message: BaseMessage):
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
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        if isinstance(message, DisconnectResponse):
            logging.error("Worker initiated DisconnectRequest got replied")
            self._task.cancel()
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        await self._connector_external.connect(self._address, ConnectorRemoteType.Binder)

        if self._object_storage_address is not None:
            # With a manually set storage address, immediately connect to the object storage server.
            await self._connector_storage.connect(self._object_storage_address)

        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.process_task, 0),
                create_async_loop_routine(self._task_manager.resolve_tasks, 0),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"{self.identity!r}: {str(e)}")
        except Exception as e:
            logging.exception(f"{self.identity!r}: failed with unhandled exception:\n{e}")

        if isinstance(self._backend, ZMQNetworkBackend):
            await self.__graceful_shutdown()

        self._connector_external.destroy()
        self._connector_storage.destroy()
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
