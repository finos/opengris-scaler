"""Tests for the ``_InProcessAsyncConnector`` / ``_InProcessSyncConnector`` pair
and ``InProcessAgentBridge``.

The tests exercise only the bridge primitives, not a full ClientAgent, and
patch ``scaler.client.agent.bridge._run_sync`` so the sync half of the
connector pair can be driven from plain CPython (no JSPI required).
"""

import asyncio
import threading
import unittest
from typing import Any, List
from unittest.mock import patch

from scaler.client.agent import bridge as bridge_module
from scaler.client.agent.bridge import (
    ClientAgentBridge,
    InProcessAgentBridge,
    IPCAgentBridge,
    _InProcessAsyncConnector,
    _InProcessSyncConnector,
    create_default_bridge,
)
from scaler.config.types.address import AddressConfig, SocketType
from scaler.protocol.capnp import ClientDisconnect


class _ImmediateRunSync:
    """Drop-in replacement for ``pyodide.ffi.run_sync`` that drives a coroutine
    to completion on a dedicated event loop the current thread owns.

    Real JSPI suspends the wasm stack while the asyncio loop keeps running on
    the same thread. For unit tests we use a brand-new loop per call so we can
    exercise the sync/async queue handoff without a real JSPI implementation.
    """

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()

    def __call__(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    def close(self) -> None:
        self.loop.close()


def _patched_run_sync(driver: _ImmediateRunSync):
    return patch.object(bridge_module, "_run_sync", driver)


class InProcessConnectorPairTest(unittest.TestCase):
    def setUp(self) -> None:
        self._driver = _ImmediateRunSync()
        self._loop = self._driver.loop

    def tearDown(self) -> None:
        self._driver.close()

    def _make_pair(self):
        incoming: asyncio.Queue = asyncio.Queue()
        outgoing: asyncio.Queue = asyncio.Queue()

        received: List[Any] = []

        async def callback(msg):
            received.append(msg)

        async_conn = _InProcessAsyncConnector(
            identity=b"ident", callback=callback, incoming=incoming, outgoing=outgoing
        )
        sync_conn = _InProcessSyncConnector(
            identity=b"ident",
            address=AddressConfig(SocketType.inproc, host="test"),
            incoming=incoming,
            outgoing=outgoing,
        )
        return async_conn, sync_conn, received

    def test_bind_and_connect_are_noops(self) -> None:
        async_conn, _, _ = self._make_pair()
        addr = AddressConfig(SocketType.inproc, host="x")
        self._loop.run_until_complete(async_conn.bind(addr))
        self.assertEqual(async_conn.address, addr)

    def test_sync_send_reaches_async_receive(self) -> None:
        async_conn, sync_conn, _ = self._make_pair()
        msg = ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.disconnect)

        with _patched_run_sync(self._driver):
            sync_conn.send(msg)

        got = self._loop.run_until_complete(async_conn.receive())
        self.assertIs(got, msg)

    def test_async_send_reaches_sync_receive(self) -> None:
        async_conn, sync_conn, _ = self._make_pair()
        msg = ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown)

        self._loop.run_until_complete(async_conn.send(msg))

        with _patched_run_sync(self._driver):
            got = sync_conn.receive()

        self.assertIs(got, msg)

    def test_routine_dispatches_to_callback(self) -> None:
        async_conn, sync_conn, received = self._make_pair()
        msg = ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.disconnect)

        with _patched_run_sync(self._driver):
            sync_conn.send(msg)

        self._loop.run_until_complete(async_conn.routine())
        self.assertEqual(received, [msg])

    def test_routine_returns_on_sentinel(self) -> None:
        async_conn, _, received = self._make_pair()
        async_conn.destroy()  # pushes a sentinel
        self._loop.run_until_complete(async_conn.routine())
        self.assertEqual(received, [])

    def test_destroy_wakes_pending_reads(self) -> None:
        async_conn, _, _ = self._make_pair()

        async def run() -> Any:
            reader = asyncio.create_task(async_conn.receive())
            await asyncio.sleep(0)  # let the reader park
            async_conn.destroy()
            return await reader

        result = self._loop.run_until_complete(run())
        self.assertIsNone(result)

    def test_sync_send_after_destroy_is_noop(self) -> None:
        _, sync_conn, _ = self._make_pair()
        sync_conn.destroy()
        # Should not attempt to drive the loop.
        sync_conn.send(ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.disconnect))


class CreateDefaultBridgeTest(unittest.TestCase):
    def test_native_platform_selects_ipc_bridge(self) -> None:
        # On any non-emscripten platform the default must be IPCAgentBridge.
        # We can't fully construct one here without a backend; instead assert
        # that the factory picks the class without attempting to call it.
        with patch.object(bridge_module.sys, "platform", "linux"):
            with patch.object(bridge_module, "IPCAgentBridge") as mock_cls:
                create_default_bridge(
                    identity=b"id",
                    scheduler_address=AddressConfig(SocketType.tcp, "127.0.0.1", 1),
                    network_backend=object(),  # type: ignore[arg-type]
                    future_manager=object(),  # type: ignore[arg-type]
                    stop_event=threading.Event(),
                    timeout_seconds=10,
                    heartbeat_interval_seconds=1,
                    serializer=object(),  # type: ignore[arg-type]
                )
                mock_cls.assert_called_once()

    def test_emscripten_platform_selects_in_process_bridge(self) -> None:
        with patch.object(bridge_module.sys, "platform", "emscripten"):
            with patch.object(bridge_module, "InProcessAgentBridge") as mock_cls:
                create_default_bridge(
                    identity=b"id",
                    scheduler_address=AddressConfig(SocketType.ws, "host", 1),
                    network_backend=object(),  # type: ignore[arg-type]
                    future_manager=object(),  # type: ignore[arg-type]
                    stop_event=threading.Event(),
                    timeout_seconds=10,
                    heartbeat_interval_seconds=1,
                    serializer=object(),  # type: ignore[arg-type]
                )
                mock_cls.assert_called_once()


class BridgeSurfaceParityTest(unittest.TestCase):
    """Ensure the native and browser bridges expose the same public surface,
    so an implementation drift gets caught here rather than at runtime."""

    def test_both_bridges_implement_the_abstract_methods(self) -> None:
        methods = {"start", "get_object_storage_address", "connector", "is_alive", "join"}
        for cls in (IPCAgentBridge, InProcessAgentBridge):
            for name in methods:
                self.assertTrue(hasattr(cls, name), f"{cls.__name__} is missing required bridge method {name!r}")

    def test_both_bridges_are_client_agent_bridges(self) -> None:
        self.assertTrue(issubclass(IPCAgentBridge, ClientAgentBridge))
        self.assertTrue(issubclass(InProcessAgentBridge, ClientAgentBridge))


class CheckBrowserRuntimeTest(unittest.TestCase):
    """``check_browser_runtime`` guards ``Client`` against being instantiated
    on an emscripten build that lacks JSPI (where the sync API would
    deadlock). On native platforms it must be a silent no-op."""

    def test_native_platform_is_noop(self) -> None:
        with patch.object(bridge_module.sys, "platform", "linux"):
            bridge_module.check_browser_runtime()  # must not raise

    def test_emscripten_without_jspi_raises(self) -> None:
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pyodide.ffi" or name.startswith("pyodide"):
                raise ImportError("simulated: no pyodide module")
            return real_import(name, *args, **kwargs)

        with patch.object(bridge_module.sys, "platform", "emscripten"):
            with patch.object(builtins, "__import__", fake_import):
                with self.assertRaises(RuntimeError) as ctx:
                    bridge_module.check_browser_runtime()
        # Error message should mention JSPI so users know what's missing.
        self.assertIn("JSPI", str(ctx.exception))

    def test_emscripten_with_jspi_does_not_raise(self) -> None:
        import types

        fake_ffi = types.ModuleType("pyodide.ffi")
        fake_ffi.run_sync = lambda coro: None  # type: ignore[attr-defined]
        fake_pyodide = types.ModuleType("pyodide")
        fake_pyodide.ffi = fake_ffi  # type: ignore[attr-defined]

        with patch.object(bridge_module.sys, "platform", "emscripten"):
            with patch.dict("sys.modules", {"pyodide": fake_pyodide, "pyodide.ffi": fake_ffi}):
                bridge_module.check_browser_runtime()  # must not raise


if __name__ == "__main__":
    unittest.main()
