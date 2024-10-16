import asyncio
import functools
import logging

import zmq.asyncio

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.config import CLEANUP_INTERVAL_SECONDS, STATUS_REPORT_INTERVAL_SECONDS
from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeat,
    DisconnectRequest,
    GraphTask,
    GraphTaskCancel,
    ObjectInstruction,
    ObjectRequest,
    Task,
    TaskCancel,
    TaskResult,
    WorkerHeartbeat,
)
from scaler.protocol.python.mixins import Message
from scaler.scheduler.client_manager import VanillaClientManager
from scaler.scheduler.config import SchedulerConfig
from scaler.scheduler.graph_manager import VanillaGraphTaskManager
from scaler.scheduler.object_manager import VanillaObjectManager
from scaler.scheduler.status_reporter import StatusReporter
from scaler.scheduler.task_manager import VanillaTaskManager
from scaler.scheduler.worker_manager import VanillaWorkerManager
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.zmq_config import ZMQConfig, ZMQType


class Scheduler:
    def __init__(self, config: SchedulerConfig):
        if config.address.type != ZMQType.tcp:
            raise TypeError(
                f"{self.__class__.__name__}: scheduler address must be tcp type: {config.address.to_address()}"
            )

        self._address_monitor = ZMQConfig(
            type=ZMQType.ipc, host=f"/tmp/{config.address.host}_{config.address.port}_monitor"
        )

        logging.info(f"{self.__class__.__name__}: monitor address is {self._address_monitor.to_address()}")
        self._context = zmq.asyncio.Context(io_threads=config.io_threads)
        self._binder = AsyncBinder(context=self._context, name="scheduler", address=config.address)
        self._binder_monitor = AsyncConnector(
            context=self._context,
            name="scheduler_monitor",
            socket_type=zmq.PUB,
            address=self._address_monitor,
            bind_or_connect="bind",
            callback=None,
            identity=None,
        )
        self._client_manager = VanillaClientManager(
            client_timeout_seconds=config.client_timeout_seconds, protected=config.protected
        )
        self._object_manager = VanillaObjectManager()
        self._graph_manager = VanillaGraphTaskManager()
        self._task_manager = VanillaTaskManager(max_number_of_tasks_waiting=config.max_number_of_tasks_waiting)
        self._worker_manager = VanillaWorkerManager(
            per_worker_queue_size=config.per_worker_queue_size,
            timeout_seconds=config.worker_timeout_seconds,
            load_balance_seconds=config.load_balance_seconds,
            load_balance_trigger_times=config.load_balance_trigger_times,
        )
        self._status_reporter = StatusReporter(self._binder_monitor)

        # register
        self._binder.register(self.on_receive_message)
        self._client_manager.register(
            self._binder, self._binder_monitor, self._object_manager, self._task_manager, self._worker_manager
        )
        self._object_manager.register(self._binder, self._binder_monitor, self._client_manager, self._worker_manager)
        self._graph_manager.register(
            self._binder, self._binder_monitor, self._client_manager, self._task_manager, self._object_manager
        )
        self._task_manager.register(
            self._binder,
            self._binder_monitor,
            self._client_manager,
            self._object_manager,
            self._worker_manager,
            self._graph_manager,
        )
        self._worker_manager.register(self._binder, self._binder_monitor, self._task_manager)

        self._status_reporter.register_managers(
            self._binder, self._client_manager, self._object_manager, self._task_manager, self._worker_manager
        )

    async def on_receive_message(self, source: bytes, message: Message):
        # =====================================================================================
        # receive from upstream
        if isinstance(message, ClientHeartbeat):
            await self._client_manager.on_heartbeat(source, message)
            return

        if isinstance(message, GraphTask):
            await self._graph_manager.on_graph_task(source, message)
            return

        if isinstance(message, GraphTaskCancel):
            await self._graph_manager.on_graph_task_cancel(source, message)
            return

        if isinstance(message, Task):
            await self._task_manager.on_task_new(source, message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_task_cancel(source, message)
            return

        # scheduler receives client shutdown request from upstream
        if isinstance(message, ClientDisconnect):
            await self._client_manager.on_client_disconnect(source, message)
            return

        # =====================================================================================
        # receive from downstream
        # receive worker heartbeat from downstream
        if isinstance(message, WorkerHeartbeat):
            await self._worker_manager.on_heartbeat(source, message)
            return

        # receive task result from downstream
        if isinstance(message, TaskResult):
            await self._worker_manager.on_task_result(message)
            return

        # scheduler receives worker disconnect request from downstream
        if isinstance(message, DisconnectRequest):
            await self._worker_manager.on_disconnect(source, message)
            return

        # =====================================================================================
        # object related
        # scheduler receives object request from upstream
        if isinstance(message, ObjectInstruction):
            await self._object_manager.on_object_instruction(source, message)
            return

        if isinstance(message, ObjectRequest):
            await self._object_manager.on_object_request(source, message)
            return

        logging.error(f"{self.__class__.__name__}: unknown message from {source=}: {message}")

    async def get_loops(self):
        loops = [
            create_async_loop_routine(self._binder.routine, 0),
            create_async_loop_routine(self._graph_manager.routine, 0),
            create_async_loop_routine(self._task_manager.routine, 0),
            create_async_loop_routine(self._client_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._object_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._worker_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._status_reporter.routine, STATUS_REPORT_INTERVAL_SECONDS),
        ]

        # if self._log_forwarder is not None:
        #     loops.append(create_async_loop_routine(self._log_forwarder.routine, 0))

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ClientShutdownException as e:
            logging.info(f"{self.__class__.__name__}: {e}")
            pass

        self._binder.destroy()
        self._binder_monitor.destroy()


@functools.wraps(Scheduler)
async def scheduler_main(*args, **kwargs):
    scheduler = Scheduler(*args, **kwargs)
    await scheduler.get_loops()
