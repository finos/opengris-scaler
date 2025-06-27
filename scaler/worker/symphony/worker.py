import asyncio
import logging
import multiprocessing
import signal
from typing import Optional

import zmq
import zmq.asyncio

from scaler.io.async_connector import AsyncConnector
from scaler.io.async_object_storage_connector import AsyncObjectStorageConnector
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
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager
from scaler.worker.symphony.heartbeat_manager import SymphonyHeartbeatManager
from scaler.worker.symphony.task_manager import SymphonyTaskManager


class SymphonyWorker(multiprocessing.get_context("spawn").Process):  # type: ignore
    """
    SymphonyWorker is an implementation of a worker that can handle multiple tasks concurrently.
    Most of the task execution logic is handled by SymphonyTaskManager.
    """

    def __init__(
        self,
        event_loop: str,
        name: str,
        address: ZMQConfig,
        io_threads: int,
        service_name: str,
        base_concurrency: int,
        heartbeat_interval_seconds: int,
        death_timeout_seconds: int,
    ):
        multiprocessing.Process.__init__(self, name="Agent")

        self._event_loop = event_loop
        self._name = name
        self._address = address
        self._io_threads = io_threads

        self._ident = WorkerID.generate_worker_id(name)  # _identity is internal to multiprocessing.Process

        self._service_name = service_name
        self._base_concurrency = base_concurrency

        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds

        self._context: Optional[zmq.asyncio.Context] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager: Optional[SymphonyTaskManager] = None
        self._heartbeat_manager: Optional[SymphonyHeartbeatManager] = None

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
        self._connector_external = AsyncConnector(
            context=self._context,
            name=self.name,
            socket_type=zmq.DEALER,
            address=self._address,
            bind_or_connect="connect",
            callback=self.__on_receive_external,
            identity=self._ident,
        )

        self._heartbeat_manager = SymphonyHeartbeatManager()
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
            await self._task_manager.on_object_instruction(message)
            return

        if isinstance(message, ClientDisconnect):
            if message.disconnect_type == ClientDisconnect.DisconnectType.Shutdown:
                raise ClientShutdownException("received client shutdown, quitting")
            logging.error(f"Worker received invalid ClientDisconnect type, ignoring {message=}")
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        try:
            await asyncio.gather(
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._task_manager.process_task, 0),
                create_async_loop_routine(self._task_manager.resolve_tasks, 0),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"{self.identity!r}: {str(e)}")

        await self._connector_external.send(DisconnectRequest.new_msg(self.identity))

        self._connector_external.destroy()
        logging.info(f"{self.identity!r}: quitted")

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)

    def __destroy(self):
        self._task.cancel()
