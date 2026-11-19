import abc
from concurrent.futures import Future
from typing import Any, Coroutine, Optional, TypeVar

from scaler.io.mixins import AsyncObjectStorageConnector, SyncConnector
from scaler.protocol.capnp import (
    ClientDisconnect,
    ClientHeartbeatEcho,
    ClientShutdownResponse,
    GraphTask,
    ObjectInstruction,
    Task,
    TaskCancelConfirm,
    TaskResult,
)

T = TypeVar("T")


class ClientAgentBridge(abc.ABC):
    """Bridges a synchronous ``Client`` to an asynchronous ``ClientAgent``.

    Implementations encapsulate the lifecycle of the agent (start/stop/wait)
    and expose a ``SyncConnector``-compatible handle that delivers messages
    from the ``Client`` to the agent's receive handler.
    """

    @abc.abstractmethod
    def start(self) -> None:
        """Start the agent. Must be called exactly once, before any other method."""

    @property
    @abc.abstractmethod
    def connector(self) -> SyncConnector:
        """Return the ``SyncConnector`` the ``Client`` uses to talk to the agent.

        Only valid after ``start()`` has returned.
        """

    @abc.abstractmethod
    def is_alive(self) -> bool:
        """Return True if the agent is still running."""

    @abc.abstractmethod
    def join(self) -> None:
        """Wait for the agent to fully stop. Safe to call multiple times."""

    @abc.abstractmethod
    def run_in_agent(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        """Run a coroutine on the agent's event loop, and return a future that completes with its result.

        Can be called from any thread, including from the agent's event loop itself.

        Takes ownership of the coroutine, closing it if the agent is not running anymore.
        """

    @abc.abstractmethod
    async def object_storage_connector(self) -> AsyncObjectStorageConnector:
        """Return the agent's object storage connector, once it is connected.

        Must be awaited from the agent's event loop, e.g. from within ``run_in_agent()``.
        """


class HeartbeatManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def send_heartbeat(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_heartbeat_echo(self, heartbeat: ClientHeartbeatEcho):
        raise NotImplementedError()


class TimeoutManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def update_last_seen_time(self):
        raise NotImplementedError()


class ObjectManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_object_instruction(self, object_instruction: ObjectInstruction):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    async def clear_all_objects(self, clear_serializer: bool):
        raise NotImplementedError()


class TaskManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_new_task(self, task: Task):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_new_graph_task(self, task: GraphTask):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_task_result(self, result: TaskResult):
        raise NotImplementedError()


class FutureManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add_future(self, future: Future):
        raise NotImplementedError()

    @abc.abstractmethod
    def cancel_all_futures(self, timeout_seconds: Optional[float] = None):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_all_futures_with_exception(self, exception: Exception):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_result(self, result: TaskResult):
        raise NotImplementedError()

    @abc.abstractmethod
    def on_task_cancel_confirm(self, cancel_confirm: TaskCancelConfirm):
        raise NotImplementedError()


class DisconnectManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def on_client_disconnect(self, disconnect: ClientDisconnect):
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_client_shutdown_response(self, response: ClientShutdownResponse):
        raise NotImplementedError()
