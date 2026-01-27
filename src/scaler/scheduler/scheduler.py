import asyncio
import functools
import logging

import zmq.asyncio

from scaler.config.defaults import CLEANUP_INTERVAL_SECONDS, STATUS_REPORT_INTERVAL_SECONDS
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.zmq import ZMQConfig, ZMQType
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.mixins import AsyncBinder, AsyncConnector, AsyncObjectStorageConnector
from scaler.io.utility import create_async_binder, create_async_object_storage_connector
from scaler.io.ymq.ymq import YMQException
from scaler.protocol.python.common import ObjectStorageAddress
from scaler.protocol.python.message import (
    ClientDisconnect,
    ClientHeartbeat,
    DisconnectRequest,
    GraphTask,
    InformationRequest,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskLog,
    TaskResult,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
)
from scaler.protocol.python.mixins import Message
from scaler.scheduler.controllers.balance_controller import VanillaBalanceController
from scaler.scheduler.controllers.client_controller import VanillaClientController
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.scheduler.controllers.information_controller import VanillaInformationController
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.scheduler.controllers.scaling_policies.utility import create_scaling_controller
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientShutdownException, ObjectStorageException
from scaler.utility.identifiers import ClientID, WorkerID


class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self._config_controller = VanillaConfigController(config)

        if config.scheduler_address.type != ZMQType.tcp:
            raise TypeError(
                f"{self.__class__.__name__}: scheduler address must be tcp type: \
                    {config.scheduler_address.to_address()}"
            )

        if config.object_storage_address is None:
            object_storage_address = ObjectStorageAddress.new_msg(
                host=config.scheduler_address.host, port=config.scheduler_address.port + 1
            )
        else:
            object_storage_address = ObjectStorageAddress.new_msg(
                host=config.object_storage_address.host, port=config.object_storage_address.port
            )
        self._config_controller.update_config("object_storage_address", object_storage_address)

        if config.monitor_address is None:
            monitor_address = ZMQConfig(
                type=ZMQType.tcp, host=config.scheduler_address.host, port=config.scheduler_address.port + 2
            )
        else:
            monitor_address = config.monitor_address
        self._config_controller.update_config("monitor_address", monitor_address)

        self._context = zmq.asyncio.Context(io_threads=config.worker_io_threads)

        self._binder: AsyncBinder = create_async_binder(
            self._context, name="scheduler", address=config.scheduler_address
        )

        logging.info(f"{self.__class__.__name__}: listen to scheduler address {config.scheduler_address}")

        self._connector_storage: AsyncObjectStorageConnector = create_async_object_storage_connector()
        logging.info(f"{self.__class__.__name__}: connect to object storage server {object_storage_address!r}")

        self._binder_monitor: AsyncConnector = ZMQAsyncConnector(
            context=self._context,
            name="scheduler_monitor",
            socket_type=zmq.PUB,
            address=monitor_address,
            bind_or_connect="bind",
            callback=None,
            identity=None,
        )
        logging.info(f"{self.__class__.__name__}: listen to scheduler monitor address {monitor_address.to_address()}")

        self._task_allocate_policy = config.allocate_policy.value()

        self._client_manager = VanillaClientController(config_controller=self._config_controller)
        self._object_controller = VanillaObjectController(config_controller=self._config_controller)
        self._graph_controller = VanillaGraphTaskController(config_controller=self._config_controller)
        self._task_controller = VanillaTaskController(config_controller=self._config_controller)
        self._worker_controller = VanillaWorkerController(
            config_controller=self._config_controller, task_allocate_policy=self._task_allocate_policy
        )
        self._balance_controller = VanillaBalanceController(
            config_controller=self._config_controller, task_allocate_policy=self._task_allocate_policy
        )
        self._information_controller = VanillaInformationController(config_controller=self._config_controller)
        self._scaling_controller = create_scaling_controller(
            config.scaling_controller_strategy, config.adapter_webhook_urls
        )

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

        self._information_controller.register_managers(
            self._binder_monitor,
            self._binder,
            self._client_manager,
            self._object_controller,
            self._task_controller,
            self._worker_controller,
            self._scaling_controller,
        )

    async def connect_to_storage(self):
        object_storage_address = self._config_controller.get_config("object_storage_address")
        await self._connector_storage.connect(object_storage_address.host, object_storage_address.port)

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

        # =====================================================================================
        # task manager
        if isinstance(message, Task):
            await self._task_controller.on_task_new(message)
            return

        if isinstance(message, TaskCancel):
            if self._graph_controller.is_graph_subtask(message.task_id):
                await self._graph_controller.on_graph_task_cancel(message)
            else:
                await self._task_controller.on_task_cancel(ClientID(source), message)
            return

        if isinstance(message, TaskCancelConfirm):
            await self._task_controller.on_task_cancel_confirm(message)
            return

        if isinstance(message, TaskResult):
            await self._task_controller.on_task_result(message)
            return

        if isinstance(message, TaskLog):
            client = self._client_manager.get_client_id(message.task_id)
            if client is not None:
                await self._binder.send(client, message)
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

        if isinstance(message, WorkerDisconnectNotification):
            await self._worker_controller.on_disconnect_notification(WorkerID(source), message)
            return

        # =====================================================================================
        # object manager
        if isinstance(message, ObjectInstruction):
            await self._object_controller.on_object_instruction(source, message)
            return

        # =====================================================================================
        # information manager
        if isinstance(message, InformationRequest):
            await self._information_controller.on_request(message)

        logging.error(f"{self.__class__.__name__}: unknown message from {source=}: {message}")

    async def get_loops(self):
        await self.connect_to_storage()

        loops = [
            create_async_loop_routine(self._binder.routine, 0),
            create_async_loop_routine(self._connector_storage.routine, 0),
            create_async_loop_routine(self._graph_controller.routine, 0),
            create_async_loop_routine(
                self._balance_controller.routine, self._config_controller.get_config("load_balance_seconds")
            ),
            create_async_loop_routine(self._client_manager.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._object_controller.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._worker_controller.routine, CLEANUP_INTERVAL_SECONDS),
            create_async_loop_routine(self._information_controller.routine, STATUS_REPORT_INTERVAL_SECONDS),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ClientShutdownException as e:
            logging.info(f"{self.__class__.__name__}: {e}")
            pass
        except YMQException:
            pass
        except ObjectStorageException:
            pass

        self._binder.destroy()
        self._binder_monitor.destroy()
        self._connector_storage.destroy()


@functools.wraps(Scheduler)
async def scheduler_main(*args, **kwargs):
    scheduler = Scheduler(*args, **kwargs)
    await scheduler.get_loops()
