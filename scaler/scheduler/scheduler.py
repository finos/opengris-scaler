import asyncio
import functools
import logging

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
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
    TaskResult,
    WorkerHeartbeat,
)
from scaler.protocol.python.mixins import Message
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.scheduler.allocate_policy.even_load_allocate_policy import EvenLoadAllocatePolicy
from scaler.scheduler.config import SchedulerConfig
from scaler.scheduler.managers.balance_manager import VanillaBalanceManager
from scaler.scheduler.managers.client_manager import VanillaClientManager
from scaler.scheduler.managers.graph_manager import VanillaGraphTaskManager
from scaler.scheduler.managers.information_manager import VanillaInformationManager
from scaler.scheduler.managers.object_manager import VanillaObjectManager
from scaler.scheduler.managers.task_manager import VanillaTaskManager
from scaler.scheduler.managers.worker_manager import VanillaWorkerManager
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, WorkerID
from scaler.utility.ymq_config import YMQConfig


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
            self._address_monitor = YMQConfig(host=config.address.host, port=config.address.port + 2)
        else:
            self._address_monitor = config.monitor_address

        self._context = IOContext(num_threads=config.io_threads)
        self._binder = AsyncBinder(context=self._context, name="scheduler", address=config.address)
        self._binder.init_sync()
        self._binder_monitor = AsyncConnector(
            context=self._context,
            name="scheduler_monitor",
            address=self._address_monitor,
            callback=None,
            identity=None,
        )
        self._binder_monitor.init_sync(bind_or_connect="bind", socket_type=IOSocketType.Multicast)
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

        self._client_manager = VanillaClientManager(
            client_timeout_seconds=config.client_timeout_seconds,
            protected=config.protected,
            storage_address=self._storage_address,
        )
        self._object_manager = VanillaObjectManager()
        self._graph_manager = VanillaGraphTaskManager()
        self._task_manager = VanillaTaskManager(max_number_of_tasks_waiting=config.max_number_of_tasks_waiting)
        self._worker_manager = VanillaWorkerManager(
            timeout_seconds=config.worker_timeout_seconds,
            task_allocate_policy=self._task_allocate_policy,
            storage_address=self._storage_address,
        )
        self._balance_manager = VanillaBalanceManager(
            load_balance_trigger_times=config.load_balance_trigger_times,
            task_allocate_policy=self._task_allocate_policy,
        )
        self._information_manager = VanillaInformationManager(self._binder_monitor)

        # register
        self._binder.register(self.on_receive_message)
        self._client_manager.register(
            self._binder, self._binder_monitor, self._object_manager, self._task_manager, self._worker_manager
        )
        self._object_manager.register(
            self._binder, self._binder_monitor, self._connector_storage, self._client_manager, self._worker_manager
        )
        self._graph_manager.register(
            self._binder,
            self._binder_monitor,
            self._connector_storage,
            self._client_manager,
            self._task_manager,
            self._object_manager,
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
        self._balance_manager.register(self._binder, self._binder_monitor, self._task_manager)

        self._information_manager.register_managers(
            self._binder, self._client_manager, self._object_manager, self._task_manager, self._worker_manager
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
            await self._graph_manager.on_graph_task(ClientID(source), message)
            return

        if isinstance(message, GraphTaskCancel):
            await self._graph_manager.on_graph_task_cancel(ClientID(source), message)
            return

        # =====================================================================================
        # task manager
        if isinstance(message, Task):
            await self._task_manager.on_task_new(ClientID(source), message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_task_cancel(ClientID(source), message)
            return

        # receive task result from downstream
        if isinstance(message, TaskResult):
            await self._worker_manager.on_task_result(message)
            return

        # scheduler receives client shutdown request from upstream
        if isinstance(message, ClientDisconnect):
            await self._client_manager.on_client_disconnect(ClientID(source), message)
            return

        # =====================================================================================
        # worker manager
        if isinstance(message, WorkerHeartbeat):
            await self._worker_manager.on_heartbeat(WorkerID(source), message)
            return

        # scheduler receives worker disconnect request from downstream
        if isinstance(message, DisconnectRequest):
            await self._worker_manager.on_disconnect(WorkerID(source), message)
            return

        # =====================================================================================
        # object manager
        if isinstance(message, ObjectInstruction):
            await self._object_manager.on_object_instruction(source, message)
            return

        logging.error(f"{self.__class__.__name__}: unknown message from {source=}: {message}")

    async def get_loops(self):
        await self.connect_to_storage()

        loops = [
            create_async_loop_routine(self._binder.routine, 0),
            create_async_loop_routine(self._connector_storage.routine, 0),
            create_async_loop_routine(self._graph_manager.routine, 0),
            create_async_loop_routine(self._task_manager.routine, 0),
            create_async_loop_routine(self._balance_manager.routine, self._config.load_balance_seconds),
            create_async_loop_routine(self._client_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._object_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._worker_manager.routine, CLEANUP_INTERVAL_SECONDS),
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
