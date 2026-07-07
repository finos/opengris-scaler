"""Regression tests for the YMQ WebSocket transport.

These tests pin behaviour that previously regressed:

* The C++ ``WebSocketStream::readStart`` failed to drain bytes already buffered
  from the WebSocket upgrade leftover. When a peer's first WebSocket frames
  (e.g. the YMQ handshake's magic + identity) arrived coalesced into the same
  TCP segment as the HTTP 101 upgrade response, those frames sat in the
  receive buffer forever, hanging the client. See ``websocket_stream.cpp``.

The tests below exercise the native YMQ ``ws://`` transport end-to-end
(``BinderSocket`` <-> ``ConnectorSocket``) and the higher-level
object-storage connector. They mirror the parity expectation that
``ws://`` transport must behave identically to ``tcp://`` for native peers,
since browser wasm clients exclusively speak ``ws://``.
"""

import asyncio
import os
import socket
import unittest

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.types.address import AddressConfig
from scaler.io.ymq import BinderSocket, Bytes, ConnectorSocket, IOContext
from scaler.utility.identifiers import ObjectID


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class WebSocketTransportTest(unittest.IsolatedAsyncioTestCase):
    """End-to-end ``ws://`` transport tests for native YMQ sockets."""

    async def test_basic_roundtrip(self) -> None:
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")

        address = await binder.bind_to("ws://127.0.0.1:0")
        self.assertEqual(repr(address)[:5], "ws://")

        connector = ConnectorSocket.connect(ctx, "connector", repr(address))

        await connector.send_message(Bytes(b"ws-payload"))
        msg = await asyncio.wait_for(binder.recv_message(), timeout=5.0)

        assert msg.address is not None
        self.assertEqual(msg.address.data, b"connector")
        self.assertEqual(msg.payload.data, b"ws-payload")

    async def test_bidirectional(self) -> None:
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")
        address = await binder.bind_to("ws://127.0.0.1:0")
        connector = ConnectorSocket.connect(ctx, "connector", repr(address))

        # Connector -> Binder
        await connector.send_message(Bytes(b"ping"))
        msg = await asyncio.wait_for(binder.recv_message(), timeout=5.0)
        self.assertEqual(msg.payload.data, b"ping")

        # Binder -> Connector (this exercises the path that previously hung:
        # the connector's WS read loop must drain the upgrade leftover that
        # contains the binder's handshake frames before any application data
        # can flow back).
        await binder.send_message("connector", Bytes(b"pong"))
        msg = await asyncio.wait_for(connector.recv_message(), timeout=5.0)
        self.assertEqual(msg.payload.data, b"pong")

    async def test_multiple_messages_in_quick_succession(self) -> None:
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")
        address = await binder.bind_to("ws://127.0.0.1:0")
        connector = ConnectorSocket.connect(ctx, "connector", repr(address))

        for i in range(20):
            await connector.send_message(Bytes(f"msg-{i}".encode()))

        for i in range(20):
            msg = await asyncio.wait_for(binder.recv_message(), timeout=5.0)
            self.assertEqual(msg.payload.data, f"msg-{i}".encode())


class WebSocketObjectStorageTest(unittest.TestCase):
    """End-to-end SET/GET against an OSS bound on ``ws://``.

    This is the precise scenario that previously hung in JupyterLite: a native
    YMQ client over ``ws://`` performing object-storage operations.
    """

    def test_set_get_over_websocket(self) -> None:
        os.environ["SCALER_NETWORK_BACKEND"] = "ymq"
        from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector

        port = _free_tcp_port()
        bind_addr = f"ws://127.0.0.1:{port}"
        addr = AddressConfig.from_string(bind_addr)

        proc = ObjectStorageServerProcess(
            bind_address=addr,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_level="WARNING",
            logging_config_file=None,
        )
        proc.start()

        try:
            proc.wait_until_ready()

            ctx = IOContext()
            connector = YMQSyncObjectStorageConnector(ctx, b"ws-test-client", addr)

            object_id = ObjectID(b"\x00" * 32)
            payload = b"hello-from-websocket-regression-test"

            connector.set_object(object_id, payload)
            got = connector.get_object(object_id)
            self.assertEqual(bytes(got), payload)
        finally:
            proc.terminate()
            proc.join(timeout=5.0)


if __name__ == "__main__":
    unittest.main()
