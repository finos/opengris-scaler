import asyncio
import logging

from scaler.config.defaults import CLEANUP_INTERVAL_SECONDS, STATUS_REPORT_INTERVAL_SECONDS
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.mixins import AsyncBinder, AsyncObjectStorageConnector, AsyncPublisher
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.io.ymq import YMQException
from scaler.protocol.capnp import (
    BaseMessage,
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
    WorkerHeartbeat,
    WorkerManagerCommandResponse,
    WorkerManagerHeartbeat,
)
from scaler.scheduler.controllers.balance_controller import VanillaBalanceController
from scaler.scheduler.controllers.client_controller import VanillaClientController
from scaler.scheduler.controllers.config_controller import VanillaConfigController
from scaler.scheduler.controllers.graph_controller import VanillaGraphTaskController
from scaler.scheduler.controllers.information_controller import VanillaInformationController
from scaler.scheduler.controllers.object_controller import VanillaObjectController
from scaler.scheduler.controllers.task_controller import VanillaTaskController
from scaler.scheduler.controllers.vanilla_policy_controller import VanillaPolicyController
from scaler.scheduler.controllers.worker_controller import VanillaWorkerController
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.exceptions import ClientShutdownException, ObjectStorageException
from scaler.utility.identifiers import ClientID, WorkerID


class Scheduler:
    def __init__(self, config: SchedulerConfig):
        self._config_controller = VanillaConfigController(config)

        self._identity = generate_identity_from_name("scheduler")
        self._backend = get_network_backend_from_env(io_threads=config.io_threads)

        self._address = config.bind_address

        self._binder: AsyncBinder = self._backend.create_async_binder(
            identity=self._identity, callback=self.on_receive_message
        )
        self._connector_storage: AsyncObjectStorageConnector = self._backend.create_async_object_storage_connector(
            identity=self._identity
        )
        self._binder_monitor: AsyncPublisher = self._backend.create_async_publisher(identity=self._identity)

        self._policy_controller = VanillaPolicyController(
            config.policy.policy_engine_type, config.policy.policy_content
        )

        self._client_manager = VanillaClientController(config_controller=self._config_controller)
        self._object_controller = VanillaObjectController(config_controller=self._config_controller)
        self._graph_controller = VanillaGraphTaskController(config_controller=self._config_controller)
        self._task_controller = VanillaTaskController(config_controller=self._config_controller)
        self._worker_controller = VanillaWorkerController(
            config_controller=self._config_controller, policy_controller=self._policy_controller
        )
        self._balance_controller = VanillaBalanceController(
            config_controller=self._config_controller, policy_controller=self._policy_controller
        )
        self._information_controller = VanillaInformationController(config_controller=self._config_controller)
        self._worker_manager_controller = WorkerManagerController(
            config_controller=self._config_controller, policy_controller=self._policy_controller
        )

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
        self._worker_manager_controller.register(self._binder, self._task_controller, self._worker_controller)

        self._information_controller.register_managers(
            self._binder_monitor,
            self._binder,
            self._client_manager,
            self._object_controller,
            self._task_controller,
            self._worker_controller,
            self._worker_manager_controller,
        )

    @property
    def address(self) -> AddressConfig:
        assert self._address is not None
        return self._address

    async def __initialize_network(self) -> None:
        # Scheduler's binder
        assert self._address is not None

        await self._binder.bind(self._address)

        self._address = self._binder.address
        assert self._address is not None
        self._config_controller.update_config("bind_address", self._address)

        logging.info(f"{self.__class__.__name__}: listen to scheduler address {self._address}")

        # Object storage

        await self._connector_storage.connect(self._config_controller.get_config("object_storage_address"))
        object_storage_address = self._connector_storage.address

        logging.info(f"{self.__class__.__name__}: connected to object storage server {object_storage_address!r}")

        advertised_object_storage_address = (
            self._config_controller.get_config("advertised_object_storage_address") or object_storage_address
        )

        logging.info(
            f"{self.__class__.__name__}: advertise object storage address {advertised_object_storage_address!r}"
        )

        self._config_controller.update_config("object_storage_address", object_storage_address)
        self._config_controller.update_config("advertised_object_storage_address", advertised_object_storage_address)

        # Monitor

        assert self._address.port is not None, "scheduler bind address must have a port"

        monitor_address = self._config_controller.get_config("monitor_address") or AddressConfig(
            type=SocketType.tcp, host=self._address.host, port=self._address.port + 2
        )

        await self._binder_monitor.bind(monitor_address)

        monitor_address = self._binder_monitor.address
        assert monitor_address is not None
        self._config_controller.update_config("monitor_address", monitor_address)

        logging.info(f"{self.__class__.__name__}: listen to scheduler monitor address {monitor_address!r}")

    async def on_receive_message(self, source: bytes, message: BaseMessage):
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
            if self._graph_controller.is_graph_subtask(message.taskId):
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
            client = self._client_manager.get_client_id(message.taskId)
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

        # =====================================================================================
        # object manager
        if isinstance(message, ObjectInstruction):
            await self._object_controller.on_object_instruction(source, message)
            return

        # =====================================================================================
        # information manager
        if isinstance(message, InformationRequest):
            await self._information_controller.on_request(message)

        # =====================================================================================
        # worker manager controller
        if isinstance(message, WorkerManagerHeartbeat):
            await self._worker_manager_controller.on_heartbeat(source, message)
            return

        if isinstance(message, WorkerManagerCommandResponse):
            await self._worker_manager_controller.on_command_response(source, message)
            return

        logging.error(f"{self.__class__.__name__}: unknown message from {source=}: {message}")

    async def get_loops(self):
        await self.__initialize_network()

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
            create_async_loop_routine(self._worker_manager_controller.routine, CLEANUP_INTERVAL_SECONDS),
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
