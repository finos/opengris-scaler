import asyncio
import logging
import multiprocessing
import signal
from collections import deque
from typing import Dict, Optional

import zmq

from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.io.utility import create_async_object_storage_connector
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    ObjectInstruction,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker_adapter.symphony.heartbeat_manager import SymphonyHeartbeatManager
from scaler.worker_adapter.symphony.task_manager import SymphonyTaskManager


class SymphonyWorker(multiprocessing.get_context("spawn").Process):  # type: ignore
    """
    SymphonyWorker is an implementation of a worker that can handle multiple tasks concurrently.
    Most of the task execution logic is handled by SymphonyTaskManager.
    """

    def __init__(
        self,
        name: str,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageAddressConfig],
        service_name: str,
        capabilities: Dict[str, int],
        base_concurrency: int,
        heartbeat_interval_seconds: int,
        death_timeout_seconds: int,
        task_queue_size: int,
        io_threads: int,
        event_loop: str,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

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

        self._context: Optional[zmq.asyncio.Context] = None
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
        try:
            self._loop.run_until_complete(self._run())
        finally:
            self._loop.close()

    async def _run(self) -> None:
        self.__initialize()

        self._task = self._loop.create_task(self.__get_loops())
        self.__register_signal()
        await self._task

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

        self._connector_storage = create_async_object_storage_connector()

        self._heartbeat_manager = SymphonyHeartbeatManager(
            object_storage_address=self._object_storage_address,
            capabilities=self._capabilities,
            task_queue_size=self._task_queue_size,
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

    async def __on_receive_external(self, message: Message):
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
            if message.disconnect_type == ClientDisconnect.DisconnectType.Shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        if self._object_storage_address is not None:
            # With a manually set storage address, immediately connect to the object storage server.
            await self._connector_storage.connect(self._object_storage_address.host, self._object_storage_address.port)

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

        await self._connector_external.send(DisconnectRequest.new_msg(self.identity))

        self._connector_external.destroy()
        logging.info(f"{self.identity!r}: quit")

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

    def __destroy(self):
        self._task.cancel()
