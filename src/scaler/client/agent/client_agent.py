import asyncio
import logging
import sys
import threading
from concurrent.futures import Future
from typing import Optional

from scaler.client.agent.disconnect_manager import ClientDisconnectManager
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.agent.heartbeat_manager import ClientHeartbeatManager
from scaler.client.agent.object_manager import ClientObjectManager
from scaler.client.agent.task_manager import ClientTaskManager
from scaler.client.serializer.mixins import Serializer
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.ymq import YMQException
from scaler.protocol.capnp import (
    BaseMessage,
    ClientDisconnect,
    ClientHeartbeatEcho,
    ClientShutdownResponse,
    GraphTask,
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskLog,
    TaskResult,
)
from scaler.utility.event_loop import create_async_loop_routine, run_task_forever
from scaler.utility.exceptions import ClientCancelledException, ClientQuitException, ClientShutdownException
from scaler.utility.identifiers import ClientID


class ClientAgent(threading.Thread):
    def __init__(
        self,
        identity: ClientID,
        client_agent_address: AddressConfig,
        scheduler_address: AddressConfig,
        network_backend: NetworkBackend,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
        object_storage_address: Optional[str] = None,
    ):
        threading.Thread.__init__(self, daemon=True)

        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._serializer = serializer

        self._identity = identity
        self._client_agent_address = client_agent_address
        self._scheduler_address = scheduler_address
        self._network_backend = network_backend
        self._object_storage_address: Future[AddressConfig] = Future()
        if object_storage_address is not None:
            self._object_storage_address_override = AddressConfig.from_string(object_storage_address)
        else:
            self._object_storage_address_override = None

        self._future_manager = future_manager

        self._connector_internal: AsyncConnector = self._network_backend.create_async_connector(
            identity=self._identity, callback=self.__on_receive_from_client
        )

        self._connector_external: AsyncConnector = self._network_backend.create_async_connector(
            identity=self._identity, callback=self.__on_receive_from_scheduler
        )

        self._disconnect_manager: Optional[ClientDisconnectManager] = None
        self._heartbeat_manager: Optional[ClientHeartbeatManager] = None
        self._task_manager: Optional[ClientTaskManager] = None

    def __initialize(self):
        self._disconnect_manager = ClientDisconnectManager()
        self._heartbeat_manager = ClientHeartbeatManager(
            death_timeout_seconds=self._timeout_seconds, storage_address_future=self._object_storage_address
        )
        self._object_manager = ClientObjectManager(identity=self._identity)
        self._task_manager = ClientTaskManager()

        # register all managers
        self._disconnect_manager.register(
            connector_internal=self._connector_internal, connector_external=self._connector_external
        )
        self._object_manager.register(
            connector_internal=self._connector_internal, connector_external=self._connector_external
        )
        self._task_manager.register(
            connector_external=self._connector_external,
            object_manager=self._object_manager,
            future_manager=self._future_manager,
        )
        self._heartbeat_manager.register(connector_external=self._connector_external)

    def run(self):
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run())

    async def _run(self):
        self.__initialize()
        await self.__get_loops()

    def get_object_storage_address(self) -> AddressConfig:
        """Returns the object storage address, or block until it receives it."""
        if self._object_storage_address_override is not None:
            return self._object_storage_address_override
        return self._object_storage_address.result()

    async def __on_receive_from_client(self, message: BaseMessage):
        if isinstance(message, ClientDisconnect):
            await self._disconnect_manager.on_client_disconnect(message)
            return

        if isinstance(message, ObjectInstruction):
            await self._object_manager.on_object_instruction(message)
            return

        if isinstance(message, Task):
            await self._task_manager.on_new_task(message)
            return

        if isinstance(message, TaskCancel):
            await self._task_manager.on_cancel_task(message)
            return

        if isinstance(message, GraphTask):
            await self._task_manager.on_new_graph_task(message)
            return

        raise TypeError(f"Unknown {message=}")

    async def __on_receive_from_scheduler(self, message: BaseMessage):
        if isinstance(message, ClientShutdownResponse):
            await self._disconnect_manager.on_client_shutdown_response(message)
            return

        if isinstance(message, ClientHeartbeatEcho):
            await self._heartbeat_manager.on_heartbeat_echo(message)
            return

        if isinstance(message, TaskLog):
            log_type = sys.stdout if message.logType == TaskLog.LogType.stdout else sys.stderr
            print(message.content, file=log_type, end="")
            return

        if isinstance(message, TaskResult):
            await self._task_manager.on_task_result(message)
            return

        if isinstance(message, TaskCancelConfirm):
            await self._task_manager.on_task_cancel_confirm(message)
            return

        raise TypeError(f"Unknown {message=}")

    async def __get_loops(self):
        await self._connector_internal.bind(self._client_agent_address)
        await self._connector_external.connect(self._scheduler_address, ConnectorRemoteType.Binder)

        await self._heartbeat_manager.send_heartbeat()

        loops = [
            create_async_loop_routine(self._connector_external.routine, 0),
            create_async_loop_routine(self._connector_internal.routine, 0),
            create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
        ]

        exception = None
        try:
            await asyncio.gather(*loops)
        except BaseException as e:
            exception = e
        finally:
            self._stop_event.set()  # always set the stop event before setting futures' exceptions

            if not isinstance(exception, YMQException):
                try:
                    await self._object_manager.clear_all_objects(clear_serializer=True)
                except YMQException:  # Above call triggers YMQ, which may raise
                    pass

            self._connector_external.destroy()
            self._connector_internal.destroy()

        if exception is None:
            return

        if not self._object_storage_address.done():
            self._object_storage_address.set_exception(exception)

        if isinstance(exception, asyncio.CancelledError):
            logging.error("ClientAgent: async. loop cancelled")
            self._future_manager.set_all_futures_with_exception(ClientCancelledException("client cancelled"))
        elif isinstance(exception, (ClientQuitException, ClientShutdownException)):
            logging.info("ClientAgent: client quitting")
            self._future_manager.set_all_futures_with_exception(exception)
        elif isinstance(exception, (TimeoutError, YMQException)):
            logging.error(f"ClientAgent: client timeout when connecting to {self._scheduler_address!r}")
            self._future_manager.set_all_futures_with_exception(TimeoutError())
        else:
            raise exception
