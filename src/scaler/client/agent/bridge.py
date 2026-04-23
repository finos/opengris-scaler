"""Abstracts the connection between the user-facing ``Client`` and the background
``ClientAgent`` so that different execution environments can share the same
``Client`` code.

On native CPython, the ``Client`` runs in the user's thread and the
``ClientAgent`` runs its asyncio loop in a background thread. The two
communicate over an "internal" IPC socket pair (``ipc://`` or ``tcp://`` on
Windows) so the ``Client`` can submit tasks synchronously while the agent
handles network I/O and manager state on its own loop.

Browser / Pyodide environments cannot start threads and cannot create IPC
sockets, but they can run the same ``ClientAgent`` logic directly on the single
available asyncio loop (via JSPI's ``pyodide.ffi.run_sync`` to keep the
``Client``'s sync API). This module defines the interface both paths must
satisfy, and implements both the native (``IPCAgentBridge``) and browser
(``InProcessAgentBridge``) paths.
"""

from __future__ import annotations

import abc
import asyncio
import sys
import threading
import uuid
from typing import Any, Awaitable, Callable, Optional

from scaler.client.agent.client_agent import ClientAgent
from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.serializer.mixins import Serializer
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType, NetworkBackend, SyncConnector
from scaler.protocol.capnp import BaseMessage
from scaler.utility.identifiers import ClientID


class ClientAgentBridge(abc.ABC):
    """Bridges a synchronous ``Client`` to an asynchronous ``ClientAgent``.

    Implementations encapsulate the lifecycle of the agent (start/stop/wait)
    and expose a ``SyncConnector``-compatible handle that delivers messages
    from the ``Client`` to the agent's receive handler.
    """

    @abc.abstractmethod
    def start(self) -> None:
        """Start the agent. Must be called exactly once, before any other method."""

    @abc.abstractmethod
    def get_object_storage_address(self) -> AddressConfig:
        """Block until the object storage address is known and return it.

        Called once after ``start()`` to resolve the address the client will
        use for direct object-storage reads/writes.
        """

    @property
    @abc.abstractmethod
    def connector(self) -> SyncConnector:
        """Return the ``SyncConnector`` the ``Client`` uses to talk to the agent.

        Only valid after ``start()`` and ``get_object_storage_address()`` have
        returned.
        """

    @abc.abstractmethod
    def is_alive(self) -> bool:
        """Return True if the agent is still running."""

    @abc.abstractmethod
    def join(self) -> None:
        """Wait for the agent to fully stop. Safe to call multiple times."""


class IPCAgentBridge(ClientAgentBridge):
    """Native-CPython bridge that runs the ``ClientAgent`` on a background thread.

    Creates a dedicated "internal" connector address, instantiates the agent
    with that address on the bind side, and exposes a ``SyncConnector`` on the
    connect side so the ``Client`` can ``send()``/``receive()`` synchronously
    from the user's thread while the agent's loop runs concurrently.

    This is the historical behavior of ``Client.__initialize__``; the class
    only wraps it behind a common interface so the browser path can supply a
    drop-in replacement.
    """

    def __init__(
        self,
        *,
        identity: ClientID,
        scheduler_address: AddressConfig,
        network_backend: NetworkBackend,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
        object_storage_address: Optional[str] = None,
    ) -> None:
        self._identity = identity
        self._backend = network_backend

        self._client_agent_address = self._backend.create_internal_address(
            f"scaler_client_{uuid.uuid4().hex}", same_process=True
        )

        self._agent = ClientAgent(
            identity=identity,
            client_agent_address=self._client_agent_address,
            scheduler_address=scheduler_address,
            network_backend=network_backend,
            future_manager=future_manager,
            stop_event=stop_event,
            timeout_seconds=timeout_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            serializer=serializer,
            object_storage_address=object_storage_address,
        )

        self._connector: Optional[SyncConnector] = None

    def start(self) -> None:
        self._agent.start()

    def get_object_storage_address(self) -> AddressConfig:
        return self._agent.get_object_storage_address()

    @property
    def connector(self) -> SyncConnector:
        if self._connector is None:
            # Lazily create the sync connector so we don't pay the cost until the
            # agent has reported that the object-storage address is ready (which
            # matches the ordering in the pre-refactor Client.__initialize__).
            self._connector = self._backend.create_sync_connector(
                identity=self._identity,
                connector_remote_type=ConnectorRemoteType.Connector,
                address=self._client_agent_address,
            )
        return self._connector

    def is_alive(self) -> bool:
        return self._agent.is_alive()

    def join(self) -> None:
        self._agent.join()


# ---------------------------------------------------------------------------
# In-process / browser bridge.
#
# Implements the same ``ClientAgentBridge`` interface without using threads or
# real IPC sockets. The ``ClientAgent`` coroutine runs on the user's asyncio
# loop (the only loop available under Pyodide) and the two connectors linking
# the agent to the client are in-memory queues.
#
# The sync half of the connector pair blocks via ``pyodide.ffi.run_sync``
# (JSPI), which suspends the current WebAssembly stack while the asyncio loop
# continues to drive the coroutine. JSPI is required; see ``Client`` for the
# preflight check. The queues exchange ``BaseMessage`` objects directly — no
# serialization round-trip is performed between client and agent.


def _run_sync(coro: Awaitable[Any]) -> Any:
    """Drive an awaitable to completion from a synchronous stack via JSPI.

    Only valid on Pyodide with JSPI enabled; the import is guarded at call
    time so this module is safely importable in any Python environment for
    unit-testing.
    """
    from pyodide.ffi import run_sync  # type: ignore[import-not-found]

    return run_sync(coro)


_IN_PROCESS_ADDRESS: AddressConfig = AddressConfig(SocketType.inproc, host="scaler-client-agent")


class _InProcessAsyncConnector(AsyncConnector):
    """The agent-side half of the in-process connector pair.

    ``bind`` and ``connect`` are no-ops (there is no real socket to bind).
    ``routine`` pulls one message from the client->agent queue and dispatches
    it to the agent's callback, mirroring the contract of the ymq-backed
    async connector.
    """

    def __init__(
        self,
        identity: bytes,
        callback: Callable[[BaseMessage], Awaitable[None]],
        incoming: "asyncio.Queue[Optional[BaseMessage]]",
        outgoing: "asyncio.Queue[Optional[BaseMessage]]",
    ) -> None:
        self._identity = identity
        self._callback = callback
        self._incoming = incoming  # client -> agent
        self._outgoing = outgoing  # agent -> client
        self._address: Optional[AddressConfig] = None
        self._destroyed = False

    async def bind(self, address: AddressConfig) -> None:
        self._address = address

    async def connect(self, address: AddressConfig, remote_type: ConnectorRemoteType) -> None:
        self._address = address

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        # Wake up any parked readers on either side with a sentinel.
        try:
            self._incoming.put_nowait(None)
        except asyncio.QueueFull:
            pass
        try:
            self._outgoing.put_nowait(None)
        except asyncio.QueueFull:
            pass

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def send(self, message: BaseMessage) -> None:
        if self._destroyed:
            return
        await self._outgoing.put(message)

    async def receive(self) -> Optional[BaseMessage]:
        if self._destroyed:
            return None
        return await self._incoming.get()

    async def routine(self) -> None:
        message = await self.receive()
        if message is None:
            return
        await self._callback(message)


class _InProcessSyncConnector(SyncConnector):
    """The client-side half of the in-process connector pair.

    Uses JSPI's ``run_sync`` to present a synchronous API backed by the same
    ``asyncio.Queue`` objects the agent reads from / writes to.
    """

    def __init__(
        self,
        identity: bytes,
        address: AddressConfig,
        incoming: "asyncio.Queue[Optional[BaseMessage]]",
        outgoing: "asyncio.Queue[Optional[BaseMessage]]",
    ) -> None:
        self._identity = identity
        self._address = address
        self._incoming = incoming  # client -> agent (we write here)
        self._outgoing = outgoing  # agent -> client (we read from here)
        self._destroyed = False

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> AddressConfig:
        return self._address

    def send(self, message: BaseMessage) -> None:
        if self._destroyed:
            return
        _run_sync(self._incoming.put(message))

    def receive(self) -> Optional[BaseMessage]:
        if self._destroyed:
            return None
        return _run_sync(self._outgoing.get())

    def destroy(self) -> None:
        if self._destroyed:
            return
        self._destroyed = True
        try:
            self._incoming.put_nowait(None)
        except asyncio.QueueFull:
            pass


class InProcessAgentBridge(ClientAgentBridge):
    """Browser / Pyodide bridge. Runs the ``ClientAgent`` coroutine on the
    current asyncio loop instead of on a background thread, and exchanges
    messages with the ``Client`` via in-memory queues.

    Requires JSPI (``pyodide.ffi.run_sync``) so the ``Client``'s synchronous
    API still works; callers must verify JSPI is available before
    instantiating this bridge (``Client`` does the preflight check).
    """

    def __init__(
        self,
        *,
        identity: ClientID,
        scheduler_address: AddressConfig,
        network_backend: NetworkBackend,
        future_manager: ClientFutureManager,
        stop_event: threading.Event,
        timeout_seconds: int,
        heartbeat_interval_seconds: int,
        serializer: Serializer,
        object_storage_address: Optional[str] = None,
    ) -> None:
        self._identity = identity
        self._stop_event = stop_event

        # Queues carry BaseMessage objects directly; the wire protocol between
        # Client and Agent is internal and need not be serialized.
        self._client_to_agent: asyncio.Queue[Optional[BaseMessage]] = asyncio.Queue()
        self._agent_to_client: asyncio.Queue[Optional[BaseMessage]] = asyncio.Queue()

        self._sync_connector = _InProcessSyncConnector(
            identity=identity,
            address=_IN_PROCESS_ADDRESS,
            incoming=self._client_to_agent,
            outgoing=self._agent_to_client,
        )

        def _internal_factory(
            identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
        ) -> AsyncConnector:
            return _InProcessAsyncConnector(
                identity=identity,
                callback=callback,
                incoming=self._client_to_agent,
                outgoing=self._agent_to_client,
            )

        self._agent = ClientAgent(
            identity=identity,
            client_agent_address=_IN_PROCESS_ADDRESS,
            scheduler_address=scheduler_address,
            network_backend=network_backend,
            future_manager=future_manager,
            stop_event=stop_event,
            timeout_seconds=timeout_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            serializer=serializer,
            object_storage_address=object_storage_address,
            internal_connector_factory=_internal_factory,
        )

        self._task: Optional[asyncio.Task] = None
        self._running = False

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("InProcessAgentBridge.start() may only be called once")
        loop = asyncio.get_event_loop()
        # Schedule the agent's entry coroutine on the current loop. ClientAgent
        # normally has this wrapped by threading.Thread.run() -> run_task_forever,
        # but in-process we drive it as a plain asyncio task.
        self._task = loop.create_task(self._agent._run())  # noqa: SLF001
        self._running = True

    def get_object_storage_address(self) -> AddressConfig:
        # ClientAgent resolves ``_object_storage_address`` early during its
        # bring-up (immediately after receiving the scheduler's first message).
        # Block the JSPI stack until that future is resolved; the asyncio loop
        # continues to drive the agent coroutine in the background.
        if self._agent._object_storage_address_override is not None:  # noqa: SLF001
            return self._agent._object_storage_address_override  # noqa: SLF001

        async def _wait() -> AddressConfig:
            fut = self._agent._object_storage_address  # noqa: SLF001
            while not fut.done():
                await asyncio.sleep(0.01)
            return fut.result(timeout=0)

        return _run_sync(_wait())

    @property
    def connector(self) -> SyncConnector:
        return self._sync_connector

    def is_alive(self) -> bool:
        if self._task is None:
            return False
        return self._running and not self._task.done()

    def join(self) -> None:
        if self._task is None:
            return
        self._running = False

        async def _await_task() -> None:
            try:
                await self._task  # type: ignore[misc]
            except asyncio.CancelledError:
                return
            except BaseException:
                # Match IPCAgentBridge: join() swallows terminal errors so
                # ``Client.__destroy`` can finish its cleanup even when the
                # agent failed. Errors already surfaced through futures.
                return

        _run_sync(_await_task())


def create_default_bridge(
    *,
    identity: ClientID,
    scheduler_address: AddressConfig,
    network_backend: NetworkBackend,
    future_manager: ClientFutureManager,
    stop_event: threading.Event,
    timeout_seconds: int,
    heartbeat_interval_seconds: int,
    serializer: Serializer,
    object_storage_address: Optional[str] = None,
) -> ClientAgentBridge:
    """Pick the bridge implementation appropriate for the current platform.

    Native CPython → ``IPCAgentBridge`` (threaded, IPC).
    Pyodide / Emscripten → ``InProcessAgentBridge`` (single-loop, JSPI).
    """
    bridge_cls: type[ClientAgentBridge]
    if sys.platform == "emscripten":
        bridge_cls = InProcessAgentBridge
    else:
        bridge_cls = IPCAgentBridge

    return bridge_cls(
        identity=identity,
        scheduler_address=scheduler_address,
        network_backend=network_backend,
        future_manager=future_manager,
        stop_event=stop_event,
        timeout_seconds=timeout_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        serializer=serializer,
        object_storage_address=object_storage_address,
    )
