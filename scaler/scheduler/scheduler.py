import asyncio
import functools
import logging

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.io.async_connector_zmq import AsyncConnectorZMQ
from scaler.io.async_object_storage_connector import AsyncObjectStorageConnector
from scaler.io.config import CLEANUP_INTERVAL_SECONDS, STATUS_REPORT_INTERVAL_SECONDS
from scaler.protocol.python.common import ObjectStorageAddress
from scaler.io.ymq.ymq import IOContext, IOSocketType
from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeat,
    DisconnectRequest,
    GraphTask,
    GraphTaskCancel,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskLog,
    TaskResult,
    WorkerHeartbeat,
)
from scaler.protocol.python.mixins import Message
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.scheduler.allocate_policy.even_load_allocate_policy import EvenLoadAllocatePolicy
from scaler.scheduler.config import SchedulerConfig
from scaler.scheduler.controllers.balance_controller import VanillaBalanceController
from scaler.scheduler.controllers.client_controller import VanillaClientController
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.scheduler.controllers.information_controller import VanillaInformationController
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, WorkerID
from scaler.utility.ymq_config import YMQConfig
from scaler.utility.zmq_config import ZMQConfig, ZMQType

import zmq.asyncio

class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self._config = config

        if config.storage_address is None:
            self._storage_address = ObjectStorageAddress.new_msg(host=config.address.host, port=config.address.port + 1)
        else:
            self._storage_address = ObjectStorageAddress.new_msg(
                host=config.storage_address.host, port=config.storage_address.port
            )

        if config.monitor_address is None:
            self._address_monitor = ZMQConfig(type=ZMQType.tcp, host=config.address.host, port=config.address.port + 2)
        else:
            self._address_monitor = config.monitor_address

        self._context = IOContext(num_threads=config.io_threads)
        self._binder = AsyncBinder(context=self._context, name="scheduler", address=config.address)
        self._binder.init_sync()

        self._context_zmq = zmq.asyncio.Context(io_threads=config.io_threads)
        self._binder_monitor = AsyncConnectorZMQ(
            context=self._context_zmq,
            name="scheduler_monitor",
            socket_type=zmq.PUB,
            address=self._address_monitor,
            bind_or_connect="bind",
            callback=None,
            identity=None,
        )
        self._connector_storage = AsyncObjectStorageConnector()

        logging.info(f"{self.__class__.__name__}: listen to scheduler address {config.address.to_address()}")
        logging.info(
            f"{self.__class__.__name__}: connect to object storage server {self._storage_address!r}"
        )
        logging.info(
            f"{self.__class__.__name__}: listen to scheduler monitor address {self._address_monitor.to_address()}"
        )

        match config.allocate_policy:
            case AllocatePolicy.even:
                self._task_allocate_policy = EvenLoadAllocatePolicy()
            case _:
                raise ValueError(f"Unknown allocate_policy: {config.allocate_policy}")

        self._client_manager = VanillaClientController(
            client_timeout_seconds=config.client_timeout_seconds,
            protected=config.protected,
            storage_address=self._storage_address,
        )
        self._object_controller = VanillaObjectController()
        self._graph_controller = VanillaGraphTaskController()
        self._task_controller = VanillaTaskController(max_number_of_tasks_waiting=config.max_number_of_tasks_waiting)
        self._worker_controller = VanillaWorkerController(
            timeout_seconds=config.worker_timeout_seconds,
            task_allocate_policy=self._task_allocate_policy,
            storage_address=self._storage_address,
        )
        self._balance_controller = VanillaBalanceController(
            load_balance_trigger_times=config.load_balance_trigger_times,
            task_allocate_policy=self._task_allocate_policy,
        )
        self._information_manager = VanillaInformationController(self._binder_monitor)

        # register
        self._binder.register(self.on_receive_message)
        self._client_manager.register(
            self._binder, self._binder_monitor, self._object_controller, self._task_controller, self._worker_controller
        )
        self._object_controller.register(
            self._binder, self._binder_monitor, self._connector_storage, self._client_manager, self._worker_controller
        )
        self._graph_controller.register(
            self._binder,
            self._binder_monitor,
            self._connector_storage,
            self._client_manager,
            self._task_controller,
            self._object_controller,
        )
        self._task_controller.register(
            self._binder,
            self._binder_monitor,
            self._client_manager,
            self._object_controller,
            self._worker_controller,
            self._graph_controller,
        )
        self._worker_controller.register(self._binder, self._binder_monitor, self._task_controller)
        self._balance_controller.register(self._binder, self._binder_monitor, self._task_controller)

        self._information_manager.register_managers(
            self._binder, self._client_manager, self._object_controller, self._task_controller, self._worker_controller
        )

    async def connect_to_storage(self):
        await self._connector_storage.connect(self._storage_address.host, self._storage_address.port)

    async def on_receive_message(self, source: bytes, message: Message):
        # =====================================================================================
        # client manager
        if isinstance(message, ClientHeartbeat):
            await self._client_manager.on_heartbeat(ClientID(source), message)
            return

        # scheduler receives client shutdown request from upstream
        if isinstance(message, ClientDisconnect):
            await self._client_manager.on_client_disconnect(ClientID(source), message)
            return

        # =====================================================================================
        # graph manager
        if isinstance(message, GraphTask):
            await self._graph_controller.on_graph_task(ClientID(source), message)
            return

        if isinstance(message, GraphTaskCancel):
            await self._graph_controller.on_graph_task_cancel(ClientID(source), message)
            return

        # =====================================================================================
        # task manager
        if isinstance(message, Task):
            await self._task_controller.on_task_new(ClientID(source), message)
            return

        if isinstance(message, TaskCancel):
            await self._task_controller.on_task_cancel(ClientID(source), message)
            return

        if isinstance(message, TaskLog):
            client = self._client_manager.get_client_id(message.task_id)
            if client is not None:
                await self._binder.send(client, message)
            return

        # receive task result from downstream
        if isinstance(message, TaskResult):
            await self._worker_controller.on_task_result(message)
            return

        # scheduler receives client shutdown request from upstream
        if isinstance(message, ClientDisconnect):
            await self._client_manager.on_client_disconnect(ClientID(source), message)
            return

        # =====================================================================================
        # worker manager
        if isinstance(message, WorkerHeartbeat):
            await self._worker_controller.on_heartbeat(WorkerID(source), message)
            return

        # scheduler receives worker disconnect request from downstream
        if isinstance(message, DisconnectRequest):
            await self._worker_controller.on_disconnect(WorkerID(source), message)
            return

        # =====================================================================================
        # object manager
        if isinstance(message, ObjectInstruction):
            await self._object_controller.on_object_instruction(source, message)
            return

        logging.error(f"{self.__class__.__name__}: unknown message from {source=}: {message}")

    async def get_loops(self):
        await self.connect_to_storage()

        loops = [
            create_async_loop_routine(self._binder.routine, 0),
            create_async_loop_routine(self._connector_storage.routine, 0),
            create_async_loop_routine(self._graph_controller.routine, 0),
            create_async_loop_routine(self._task_controller.routine, 0),
            create_async_loop_routine(self._balance_controller.routine, self._config.load_balance_seconds),
            create_async_loop_routine(self._client_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._object_controller.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._worker_controller.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._information_manager.routine, STATUS_REPORT_INTERVAL_SECONDS),
        ]

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
