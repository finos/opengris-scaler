import asyncio
import concurrent.futures
import logging
import sys
import threading
from typing import Any, Callable, Coroutine, Optional, TypeVar

from scaler.client.agent.disconnect_manager import ClientDisconnectManager
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.agent.heartbeat_manager import ClientHeartbeatManager
from scaler.client.agent.object_manager import ClientObjectManager
from scaler.client.agent.task_manager import ClientTaskManager
from scaler.client.serializer.mixins import Serializer
from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector, ConnectorRemoteType, NetworkBackend
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
from scaler.utility.exceptions import (
    ClientCancelledException,
    ClientQuitException,
    ClientShutdownException,
    DisconnectedError,
)
from scaler.utility.identifiers import ClientID

logger = logging.getLogger(__name__)

T = TypeVar("T")


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
        internal_connector_factory: Optional["Callable[..., AsyncConnector]"] = None,
        security_config: Optional[SecurityConfig] = None,
    ):
        threading.Thread.__init__(self, daemon=True)

        self._stop_event = stop_event
        self._timeout_seconds = timeout_seconds
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._serializer = serializer
        self._security_config = security_config

        self._identity = identity
        self._client_agent_address = client_agent_address
        self._scheduler_address = scheduler_address
        self._network_backend = network_backend

        self._object_storage_address: concurrent.futures.Future[AddressConfig] = concurrent.futures.Future()

        if object_storage_address is not None:
            self._object_storage_address.set_result(AddressConfig.from_string(object_storage_address))

        # The loop is started by the agent, but the client's thread needs it to schedule coroutines on the agent. A
        # future safely publishes it across both threads, and lets the client wait if it gets there first. The client
        # does get there first when the object storage address is known upfront, as it then does not wait on the
        # scheduler before submitting objects.
        self._loop: concurrent.futures.Future[asyncio.AbstractEventLoop] = concurrent.futures.Future()
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        self._future_manager = future_manager

        # In the native path both connectors go through the network backend.
        # The in-process bridge for browser clients supplies a factory for the
        # *internal* connector so it can wire client<->agent message flow
        # through an in-memory queue instead of a real IPC socket; the factory
        # has the same signature as ``network_backend.create_async_connector``.
        self._internal_connector_is_in_process = internal_connector_factory is not None
        create_internal = internal_connector_factory or self._network_backend.create_async_connector

        self._connector_internal: AsyncConnector = create_internal(
            identity=self._identity, callback=self.__on_receive_from_client
        )

        self._connector_external: AsyncConnector = self._network_backend.create_async_connector(
            identity=self._identity, callback=self.__on_receive_from_scheduler
        )

        self._disconnect_manager: Optional[ClientDisconnectManager] = None
        self._heartbeat_manager: Optional[ClientHeartbeatManager] = None
        self._task_manager: Optional[ClientTaskManager] = None

    def __initialize(self):
        self._connector_storage = self._network_backend.create_async_object_storage_connector(identity=self._identity)

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
        run_task_forever(asyncio.new_event_loop(), self._run())

    async def _run(self):
        self._loop.set_result(asyncio.get_running_loop())

        self.__initialize()
        await self.__get_loops()

    def run_in_agent(self, coroutine: Coroutine[Any, Any, T]) -> concurrent.futures.Future[T]:
        """Schedules a coroutine on the agent's event loop, returns a future that completes with its result.

        Can be called from any thread, but blocks until the agent starts its event loop. Raises a `RuntimeError` if
        the agent's loop is closed.
        """
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop.result())

    async def get_object_storage_connector(self) -> AsyncObjectStorageConnector:
        """Returns the agent's object storage connector, or blocks until it is connected."""

        assert self._connector_storage is not None
        await self._connector_storage.wait_until_connected()
        return self._connector_storage

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

    async def __connect_object_storage(self):
        """Connects the object storage connector, or waits until the scheduler provides its address."""

        object_storage_address = await asyncio.wrap_future(self._object_storage_address)

        logger.info(f"{self.__class__.__name__}: connect to object storage at {object_storage_address}")
        await self._connector_storage.connect(object_storage_address, security_config=self._security_config)

    async def __get_loops(self):
        exception = None
        try:
            await self._connector_internal.bind(self._client_agent_address)

            logger.info(f"{self.__class__.__name__}: connect to scheduler at {self._scheduler_address}")
            await self._connector_external.connect(
                self._scheduler_address, ConnectorRemoteType.Binder, security_config=self._security_config
            )

            await self._heartbeat_manager.send_heartbeat()

            loops = [
                self.__connect_object_storage(),
                create_async_loop_routine(self._connector_storage.routine, 0),
                create_async_loop_routine(self._connector_external.routine, 0),
                create_async_loop_routine(self._connector_internal.routine, 0),
                create_async_loop_routine(self._heartbeat_manager.routine, self._heartbeat_interval_seconds),
            ]

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
            self._connector_storage.destroy()

        if exception is None:
            return

        public_exception: BaseException
        if isinstance(exception, asyncio.CancelledError):
            # asyncio.CancelledError is a BaseException (not Exception) in Python 3.8+, so it
            # cannot go into set_all_futures_with_exception directly. Translate to the
            # public-facing ClientCancelledException.
            logger.error("ClientAgent: async. loop cancelled")
            cancelled = ClientCancelledException("client cancelled")
            self._future_manager.set_all_futures_with_exception(cancelled)
            public_exception = cancelled
        elif isinstance(exception, (ClientQuitException, ClientShutdownException)):
            logger.info("ClientAgent: client quitting")
            self._future_manager.set_all_futures_with_exception(exception)
            public_exception = exception
        elif isinstance(exception, TimeoutError):
            # The scheduler went silent for death_timeout_seconds; keep its descriptive message rather
            # than replacing it with a bare TimeoutError().
            logger.error(f"ClientAgent: lost contact with scheduler {self._scheduler_address!r}: {exception}")
            self._future_manager.set_all_futures_with_exception(exception)
            public_exception = exception
        elif isinstance(exception, YMQException):
            # The scheduler closed the connection outright (it crashed, evicted this client, or shut
            # down). Fail the in-flight tasks with a clear disconnection error naming the scheduler,
            # not a bare TimeoutError() that reads as if the tasks themselves timed out.
            logger.error(f"ClientAgent: connection to scheduler {self._scheduler_address!r} closed by remote end")
            disconnected = DisconnectedError(
                f"client lost its connection to scheduler {self._scheduler_address!r} before all tasks finished"
            )
            self._future_manager.set_all_futures_with_exception(disconnected)
            public_exception = disconnected
        else:
            public_exception = exception

        if not self._object_storage_address.done():
            self._object_storage_address.set_exception(public_exception)

        if not isinstance(
            exception,
            (asyncio.CancelledError, ClientQuitException, ClientShutdownException, TimeoutError, YMQException),
        ):
            raise exception
