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
import concurrent.futures
import sys
import threading
import time
import uuid
from typing import Any, Awaitable, Callable, Iterable, Iterator, Optional

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


# Periodic-yield hook for the browser bridge.
#
# Even with the ``concurrent.futures`` JSPI patch below, user code can still
# starve the agent task in two cases:
#
#   * pure-Python compute that never blocks on a scaler/concurrent future
#     (e.g. graph construction, large numpy preparations, pandas pipelines
#     in the gallery scaler notebooks before any submit() is called);
#
#   * any third-party library that doesn't go through concurrent.futures.
#
# Without yielding, the asyncio loop never runs, the agent can't send
# heartbeats, the scheduler hits ``client_timeout_seconds`` (default 60s)
# and disconnects the client mid-computation. The scheduler then sees late
# results / object writes from "unknown" clients and the user's submitted
# futures hang or fail.
#
# Install a ``sys.setprofile`` hook on emscripten that pumps the asyncio
# loop via ``_run_sync(asyncio.sleep(0))`` at most once every
# ``_YIELD_MIN_INTERVAL_SECONDS``. ``setprofile`` only fires on Python
# function call/return events, so the overhead is bounded (one wall-clock
# check per call) and it covers realistic numpy/pandas/user-function
# workloads. Pure-arithmetic tight loops with no function calls bypass the
# hook; users with those need to insert an explicit yield.
#
# This complements (does not replace) the ``concurrent.futures.wait``
# JSPI-aware patch below: the patch handles the case where user code blocks
# inside ``threading.Event.wait`` (pargraph), the setprofile hook handles
# the case where user code is busy in Python without blocking.

# Send a heartbeat at most every ``_YIELD_MIN_INTERVAL_SECONDS`` from the
# profile hook. 5s is well under the default 60s ``client_timeout_seconds``
# so even a single hook firing per interval keeps the scheduler happy.
_YIELD_MIN_INTERVAL_SECONDS: float = 5.0
_yield_state: dict = {
    "last": 0.0,
    "in_progress": False,
    "previous_profile": None,
    "installed": False,
    # Optional reference to the ClientAgent so the hook can send heartbeats
    # directly via ``_run_sync(heartbeat_manager.send_heartbeat())`` rather
    # than relying on the asyncio loop to tick the heartbeat task on its
    # own schedule.
    "agent": None,
    # Diagnostic counters — exposed for debugging from the notebook via
    # ``from scaler.client.agent import bridge; print(bridge._yield_state)``.
    "fire_count": 0,
    "hb_ok": 0,
    "hb_err": 0,
    "last_error": None,
}


async def _emit_heartbeat(agent: Any) -> None:
    hb = getattr(agent, "_heartbeat_manager", None) if agent is not None else None
    if hb is not None and getattr(hb, "_connector_external", None) is not None:
        await hb.send_heartbeat()
    # Always also tick the loop once so any other pending agent work
    # (object storage acks, future completions) gets a chance to run.
    await asyncio.sleep(0)


def _yield_profile_hook(frame: Any, event: str, arg: Any) -> None:
    if _yield_state["in_progress"]:
        return
    # Fire on both call AND return events to maximize coverage. The
    # throttle below bounds the actual work to once per
    # ``_YIELD_MIN_INTERVAL_SECONDS``.
    if event not in ("call", "c_call", "return", "c_return"):
        return
    now = time.monotonic()
    if now - _yield_state["last"] < _YIELD_MIN_INTERVAL_SECONDS:
        return
    _yield_state["last"] = now
    _yield_state["fire_count"] += 1
    _yield_state["in_progress"] = True
    try:
        agent = _yield_state.get("agent")
        try:
            _run_sync(_emit_heartbeat(agent))
            _yield_state["hb_ok"] += 1
        except BaseException as exc:
            _yield_state["hb_err"] += 1
            _yield_state["last_error"] = repr(exc)
            # Fall back to a bare loop tick — some run_sync failures
            # (e.g. transient JSPI re-entry guards) may still let the
            # simpler sleep(0) succeed.
            try:
                _run_sync(asyncio.sleep(0))
            except BaseException:
                pass
    except BaseException:
        # Never raise out of a profile hook — that would corrupt the
        # interpreter's profiling state and could kill the user's notebook.
        pass
    finally:
        _yield_state["in_progress"] = False


def _install_yield_hook(agent: Any = None) -> None:
    if _yield_state["installed"]:
        # Refresh agent reference in case it was installed before the agent
        # was fully constructed.
        if agent is not None:
            _yield_state["agent"] = agent
        # Re-assert the profile hook — IPython / pyodide-kernel may reset
        # ``sys.setprofile`` between cells, in which case we'd silently
        # stop firing. Re-installing here is cheap and idempotent.
        if sys.getprofile() is not _yield_profile_hook:
            _yield_state["previous_profile"] = sys.getprofile()
            sys.setprofile(_yield_profile_hook)
        return
    _yield_state["previous_profile"] = sys.getprofile()
    _yield_state["last"] = time.monotonic()
    _yield_state["agent"] = agent
    _yield_state["installed"] = True
    sys.setprofile(_yield_profile_hook)


def _uninstall_yield_hook() -> None:
    if not _yield_state["installed"]:
        return
    sys.setprofile(_yield_state["previous_profile"])
    _yield_state["previous_profile"] = None
    _yield_state["agent"] = None
    _yield_state["installed"] = False


# JSPI-aware patches for ``concurrent.futures.wait`` / ``as_completed``.
#
# The agent coroutine runs on the same single-threaded asyncio loop as the
# user's notebook code in the browser. ``ScalerFuture._wait_result_ready``
# already suspends the wasm stack via JSPI when ``.result()`` is called, so
# the loop keeps running and the agent keeps sending heartbeats. But code
# that blocks on multiple futures via the standard library — most notably
# ``pargraph.GraphEngine.get`` which calls
# ``concurrent.futures.wait(..., return_when=FIRST_COMPLETED)`` — uses
# ``threading.Event.wait`` internally, which blocks the only thread without
# letting the loop run. The agent never gets to send heartbeats, the
# scheduler trips ``client_timeout_seconds`` (60s default), and the client
# is disconnected mid-computation.
#
# When the browser bridge starts, monkey-patch ``concurrent.futures.wait``
# and ``concurrent.futures.as_completed`` to drive the asyncio loop via JSPI
# while waiting. The patch only activates on ``sys.platform == "emscripten"``
# and is idempotent.

_concurrent_futures_patched: bool = False
_original_wait: Optional[Callable[..., concurrent.futures._base.DoneAndNotDoneFutures]] = None
_original_as_completed: Optional[Callable[..., Iterator[concurrent.futures.Future]]] = None


def _jspi_wait(
    fs: Iterable[concurrent.futures.Future],
    timeout: Optional[float] = None,
    return_when: str = concurrent.futures.ALL_COMPLETED,
) -> concurrent.futures._base.DoneAndNotDoneFutures:
    fs = list(fs)
    if not fs:
        return concurrent.futures._base.DoneAndNotDoneFutures(set(), set())

    asyncio_return_when = {
        concurrent.futures.FIRST_COMPLETED: asyncio.FIRST_COMPLETED,
        concurrent.futures.FIRST_EXCEPTION: asyncio.FIRST_EXCEPTION,
        concurrent.futures.ALL_COMPLETED: asyncio.ALL_COMPLETED,
    }[return_when]

    async def _await() -> None:
        wrapped = [asyncio.wrap_future(f) for f in fs]
        # ``asyncio.wait`` registers callbacks on each wrapped future and
        # returns once ``return_when`` is satisfied (or ``timeout`` elapses).
        # We deliberately do NOT cancel the wrappers afterwards: cancelling
        # an ``asyncio.wrap_future`` wrapper propagates ``cancel()`` to the
        # underlying ``concurrent.futures.Future``, which would silently kill
        # the user's in-flight tasks. The wrappers fall out of scope and are
        # GC'd; their done callbacks are no-ops once the original future
        # completes.
        await asyncio.wait(wrapped, timeout=timeout, return_when=asyncio_return_when)

    _run_sync(_await())

    done: set = set()
    not_done: set = set()
    for f in fs:
        if f.done():
            done.add(f)
        else:
            not_done.add(f)
    return concurrent.futures._base.DoneAndNotDoneFutures(done, not_done)


def _jspi_as_completed(
    fs: Iterable[concurrent.futures.Future], timeout: Optional[float] = None
) -> Iterator[concurrent.futures.Future]:
    fs = list(fs)
    pending = set(fs)
    # Yield any already-completed futures up front, mirroring stdlib semantics.
    for f in list(pending):
        if f.done():
            pending.discard(f)
            yield f

    while pending:
        result = _jspi_wait(pending, timeout=timeout, return_when=concurrent.futures.FIRST_COMPLETED)
        if not result.done:
            raise concurrent.futures.TimeoutError(f"{len(pending)} (of {len(fs)}) futures unfinished")
        for f in result.done:
            pending.discard(f)
            yield f


def _rebind_in_loaded_modules(old_wait: Any, old_as_completed: Any, new_wait: Any, new_as_completed: Any) -> None:
    # ``from concurrent.futures import wait`` captures a local reference at
    # the importing module's load time, so rebinding ``concurrent.futures.wait``
    # later doesn't affect callers that already imported it (pargraph does
    # exactly this). Walk every loaded module and replace any attribute that
    # still points at the original function. ``concurrent.futures._base``
    # holds the canonical definitions and re-export, so it's covered too.
    for module in list(sys.modules.values()):
        if module is None:
            continue
        try:
            module_dict = getattr(module, "__dict__", None)
            if module_dict is None:
                continue
            for name, value in list(module_dict.items()):
                if value is old_wait:
                    module_dict[name] = new_wait
                elif value is old_as_completed:
                    module_dict[name] = new_as_completed
        except Exception:
            # Some modules raise on __dict__ access (lazy importers, C
            # extensions with dynamic attribute lookup); skip them.
            continue


def _install_concurrent_futures_jspi_patch() -> None:
    global _concurrent_futures_patched, _original_wait, _original_as_completed
    if _concurrent_futures_patched:
        return
    _original_wait = concurrent.futures.wait
    _original_as_completed = concurrent.futures.as_completed
    _rebind_in_loaded_modules(_original_wait, _original_as_completed, _jspi_wait, _jspi_as_completed)
    _concurrent_futures_patched = True


def _uninstall_concurrent_futures_jspi_patch() -> None:
    global _concurrent_futures_patched, _original_wait, _original_as_completed
    if not _concurrent_futures_patched:
        return
    if _original_wait is not None and _original_as_completed is not None:
        _rebind_in_loaded_modules(_jspi_wait, _jspi_as_completed, _original_wait, _original_as_completed)
    _original_wait = None
    _original_as_completed = None
    _concurrent_futures_patched = False


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

        def _internal_factory(identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]) -> AsyncConnector:
            return _InProcessAsyncConnector(
                identity=identity, callback=callback, incoming=self._client_to_agent, outgoing=self._agent_to_client
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
        # Make ``concurrent.futures.wait`` / ``as_completed`` JSPI-aware so
        # libraries like pargraph (which block on multiple futures via the
        # standard library) keep the asyncio loop running and let the agent
        # send heartbeats. Only active on emscripten because run_sync
        # requires JSPI.
        if sys.platform == "emscripten":
            _install_concurrent_futures_jspi_patch()
            _install_yield_hook(self._agent)

    def get_object_storage_address(self) -> AddressConfig:
        # ClientAgent resolves ``_object_storage_address`` early during its
        # bring-up (immediately after receiving the scheduler's first message).
        # Block the JSPI stack until that future is resolved; the asyncio loop
        # continues to drive the agent coroutine in the background.
        if self._agent._object_storage_address_override is not None:  # noqa: SLF001
            return self._agent._object_storage_address_override  # noqa: SLF001

        async def _wait() -> AddressConfig:
            fut = self._agent._object_storage_address  # noqa: SLF001
            # ``fut`` is a ``concurrent.futures.Future``. ``asyncio.wrap_future``
            # adapts it to an awaitable on the current loop without any
            # polling — the agent task signals completion in the same loop, so
            # awaiting the wrapped future yields back to asyncio exactly once
            # and resumes when the future is set. A previous version used
            # ``while not fut.done(): await asyncio.sleep(0.01)``, which under
            # ``pyodide.ffi.run_sync`` (JSPI) created a long chain of nested
            # ``setTimeout`` callbacks and could trigger Pyodide WebLoop
            # crashes ("memory access out of bounds" / "null function").
            return await asyncio.wrap_future(fut)

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
        if sys.platform == "emscripten":
            _uninstall_yield_hook()
            _uninstall_concurrent_futures_jspi_patch()

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


def check_browser_runtime() -> None:
    """Raise ``RuntimeError`` if the current runtime cannot host a ``Client``.

    The browser bridge (``InProcessAgentBridge``) drives the agent coroutine
    on the same event loop as the user and uses JavaScript Promise
    Integration (``pyodide.ffi.run_sync``) to keep ``Client``'s synchronous
    public API working. When JSPI is not available the sync API would
    deadlock, so we fail fast with an actionable error instead.

    On non-emscripten platforms this is a no-op.
    """
    if sys.platform != "emscripten":
        return

    try:
        from pyodide.ffi import run_sync  # type: ignore[import-not-found]  # noqa: F401
    except ImportError as exc:
        raise RuntimeError(
            "Scaler's browser client requires Pyodide's JavaScript Promise Integration (JSPI). "
            "pyodide.ffi.run_sync could not be imported. "
            "Please use a Pyodide build that exposes JSPI (Pyodide 0.27+ with a JSPI-capable browser, "
            "e.g. Chrome/Edge 137+). Alternatively, use 'await client.submit(...)' instead of the "
            "blocking sync API."
        ) from exc
