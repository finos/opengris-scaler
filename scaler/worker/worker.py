import asyncio
import logging
import multiprocessing
import signal
from typing import Optional, Tuple

import zmq.asyncio

from scaler.io.async_connector import AsyncConnector
from scaler.io.config import PROFILING_INTERVAL_SECONDS
from scaler.protocol.python.message import (
    ClientDisconnect,
    DisconnectRequest,
    ObjectInstruction,
    ObjectResponse,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
)
from scaler.protocol.python.mixins import Message
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.logging.utility import setup_logger
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.agent.heartbeat_manager import VanillaHeartbeatManager
from scaler.worker.agent.object_tracker import VanillaObjectTracker
from scaler.worker.agent.processor_manager import VanillaProcessorManager
from scaler.worker.agent.profiling_manager import VanillaProfilingManager
from scaler.worker.agent.task_manager import VanillaTaskManager
from scaler.worker.agent.timeout_manager import VanillaTimeoutManager


class Worker(multiprocessing.get_context("spawn").Process):  # type: ignore
    def __init__(
        self,
        event_loop: str,
        name: str,
        address: ZMQConfig,
        io_threads: int,
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
        self._io_threads = io_threads

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
        self._task_manager: Optional[VanillaTaskManager] = None
        self._heartbeat_manager: Optional[VanillaHeartbeatManager] = None
        self._profiling_manager: Optional[VanillaProfilingManager] = None
        self._processor_manager: Optional[VanillaProcessorManager] = None

    @property
    def identity(self):
        return self._connector_external.identity

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
            identity=None,
        )

        self._heartbeat_manager = VanillaHeartbeatManager()
        self._profiling_manager = VanillaProfilingManager()
        self._task_manager = VanillaTaskManager(task_timeout_seconds=self._task_timeout_seconds)
        self._timeout_manager = VanillaTimeoutManager(death_timeout_seconds=self._death_timeout_seconds)
        self._processor_manager = VanillaProcessorManager(
            context=self._context,
            event_loop=self._event_loop,
            garbage_collect_interval_seconds=self._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=self._trim_memory_threshold_bytes,
            hard_processor_suspend=self._hard_processor_suspend,
            logging_paths=self._logging_paths,
            logging_level=self._logging_level,
        )
        self._object_tracker = VanillaObjectTracker()

        # register
        self._task_manager.register(connector=self._connector_external, processor_manager=self._processor_manager)
        self._heartbeat_manager.register(
            connector_external=self._connector_external,
            worker_task_manager=self._task_manager,
            timeout_manager=self._timeout_manager,
            processor_manager=self._processor_manager,
        )
        self._processor_manager.register(
            heartbeat=self._heartbeat_manager,
            task_manager=self._task_manager,
            profiling_manager=self._profiling_manager,
            object_tracker=self._object_tracker,
            connector_external=self._connector_external,
        )
        self._processor_manager.initialize()

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
            await self._processor_manager.on_object_instruction(message)
            return

        if isinstance(message, ObjectResponse):
            await self._processor_manager.on_object_response(message)
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
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
                create_async_loop_routine(self._timeout_manager.routine, 1),
                create_async_loop_routine(self._task_manager.routine, 0),
                create_async_loop_routine(self._profiling_manager.routine, PROFILING_INTERVAL_SECONDS),
                create_async_loop_routine(self._processor_manager.routine, 0),
            )
        except asyncio.CancelledError:
            pass
        except (ClientShutdownException, TimeoutError) as e:
            logging.info(f"Worker[{self.pid}]: {str(e)}")

        await self._connector_external.send(DisconnectRequest.new_msg(self._connector_external.identity))

        self._connector_external.destroy()
        self._processor_manager.destroy("quited")
        logging.info(f"Worker[{self.pid}]: quited")

    def __run_forever(self):
        self._loop.run_until_complete(self._task)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)

    def __destroy(self):
        self._task.cancel()
