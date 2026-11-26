import asyncio
import pathlib
import socket
import struct
import unittest

from scaler.config.types.address import AddressConfig
from scaler.io.ymq import (
    MAGIC_STRING,
    BinderSocket,
    Bytes,
    ConnectionLostError,
    ConnectorSocket,
    ErrorCode,
    InvalidAddressFormatError,
    IOContext,
    TLSConfig,
)

# Reuse the self-signed certificate/key pair shipped for the C++ OpenSSL tests.
_OPENSSL_TEST_DIR = pathlib.Path(__file__).resolve().parents[2] / "cpp" / "wrapper" / "openssl"
_CERT_CHAIN = str(_OPENSSL_TEST_DIR / "sample_cert.pem")
_PRIVATE_KEY = str(_OPENSSL_TEST_DIR / "sample_private_key.pem")

# More than loopback's buffers hold for a peer that does not read, Windows' 16 MB maximum receive window included.
_STALLED_SEND_BYTES = 64 * 1024 * 1024
# Windows completes at most two sends to a peer that does not read, whatever their size (Microsoft KB 214397).
_STALLED_SENDS = 3
_SEND_SETTLE_SECONDS = 0.2
_SEND_FAIL_TIMEOUT_SECONDS = 5.0

# Peer bytes that make a YMQ socket abort the connection itself, so the abort cancels the in-flight write on every OS.
# A reset is not enough: the OS can fail the write first, which reports success (see MessageConnection::onWriteDone).
_INVALID_MAGIC_STRING = b"HTTP/1.1 200 OK\n"
_UNALLOCATABLE_FRAME_HEADER = struct.pack("<Q", 2**64 - 1)  # no buffer holds a frame this long


def _ymq_frame(payload: bytes) -> bytes:
    return struct.pack("<Q", len(payload)) + payload


class TestSockets(unittest.IsolatedAsyncioTestCase):
    async def test_basic(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")
        self.assertEqual(binder.identity, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector = ConnectorSocket.connect(ctx, "connector", repr(address))
        self.assertEqual(connector.identity, "connector")

        await connector.send_message(Bytes(b"payload"))
        msg = await binder.recv_message()

        assert msg.address is not None
        self.assertEqual(msg.address.data, b"connector")
        self.assertEqual(msg.payload.data, b"payload")

    async def test_connector_send_under_an_aborted_connection_raises_connection_lost(self):
        """A connection lost under a send raises ConnectionLostError, and the connector can send again."""
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen()
            listener.settimeout(_SEND_FAIL_TIMEOUT_SECONDS)

            connector = ConnectorSocket.connect(
                IOContext(), "connector", f"tcp://127.0.0.1:{listener.getsockname()[1]}"
            )
            self.addCleanup(connector.shutdown)
            peer, _ = listener.accept()

            with peer:
                payload = b"x" * _STALLED_SEND_BYTES
                sends = [asyncio.ensure_future(connector.send_message(Bytes(payload))) for _ in range(_STALLED_SENDS)]
                await self._assert_abort_raises_connection_lost(peer, _INVALID_MAGIC_STRING, sends)

            # the connector was not stopped: it reconnects, and the next send arrives
            new_peer, _ = await asyncio.get_running_loop().run_in_executor(None, listener.accept)
            with new_peer:
                new_peer.settimeout(_SEND_FAIL_TIMEOUT_SECONDS)
                await connector.send_message(Bytes(b"again"))
                expected = MAGIC_STRING + _ymq_frame(b"connector") + _ymq_frame(b"again")
                received = b""
                while len(received) < len(expected):
                    received += await asyncio.get_running_loop().run_in_executor(None, new_peer.recv, len(expected))
                self.assertEqual(received, expected)

    async def test_binder_send_under_an_aborted_connection_raises_connection_lost(self):
        """A connection lost under a binder's send raises ConnectionLostError, since nobody stopped the binder."""
        binder = BinderSocket(IOContext(), "binder")
        self.addCleanup(binder.shutdown)
        address = await binder.bind_to("tcp://127.0.0.1:0")

        with socket.create_connection(("127.0.0.1", AddressConfig.from_string(repr(address)).port)) as peer:
            peer.sendall(MAGIC_STRING + _ymq_frame(b"peer"))

            # queued until the peer has identified itself, then in flight, as the peer never reads
            payload = b"x" * _STALLED_SEND_BYTES
            sends = [asyncio.ensure_future(binder.send_message("peer", Bytes(payload))) for _ in range(_STALLED_SENDS)]
            await self._assert_abort_raises_connection_lost(peer, _UNALLOCATABLE_FRAME_HEADER, sends)

    async def _assert_abort_raises_connection_lost(
        self, peer: socket.socket, abort_bytes: bytes, sends: list[asyncio.Task[None]]
    ) -> None:
        await asyncio.sleep(_SEND_SETTLE_SECONDS)
        self.assertFalse(sends[-1].done(), f"the peer never reads, so the last send should be in flight: {sends[-1]!r}")

        peer.sendall(abort_bytes)

        with self.assertRaises(ConnectionLostError) as exc:
            await asyncio.wait_for(sends[-1], _SEND_FAIL_TIMEOUT_SECONDS)
        self.assertEqual(exc.exception.code, ErrorCode.ConnectionLost)

        # the earlier sends succeed where the OS took them whole (Windows), else they raise ConnectionLostError too
        for result in await asyncio.gather(*sends[:-1], return_exceptions=True):
            self.assertTrue(result is None or isinstance(result, ConnectionLostError), repr(result))

    async def test_invalid_address(self):
        ctx = IOContext()

        with self.assertRaises(InvalidAddressFormatError) as exc:
            await BinderSocket(ctx, "binder").bind_to("invalid_address")
        self.assertEqual(exc.exception.code, ErrorCode.InvalidAddressFormat)

    async def test_routing(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector1 = ConnectorSocket.connect(ctx, "connector1", repr(address))
        connector2 = ConnectorSocket.connect(ctx, "connector2", repr(address))

        await binder.send_message("connector2", Bytes(b"2"))
        await binder.send_message("connector1", Bytes(b"1"))

        msg1 = await connector1.recv_message()
        self.assertEqual(msg1.payload.data, b"1")

        msg2 = await connector2.recv_message()
        self.assertEqual(msg2.payload.data, b"2")

    async def test_pingpong(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector = ConnectorSocket.connect(ctx, "connector", repr(address))

        async def binder_routine(binder: BinderSocket, limit: int) -> bool:
            i = 0
            while i < limit:
                await binder.send_message("connector", Bytes(f"{i}".encode()))
                msg = await binder.recv_message()
                assert msg.payload.data is not None

                recv_i = int(msg.payload.data.decode())
                if recv_i - i > 1:
                    return False
                i = recv_i + 1
            return True

        async def connector_routine(connector: ConnectorSocket, limit: int) -> bool:
            i = 0
            while True:
                msg = await connector.recv_message()
                assert msg.payload.data is not None
                recv_i = int(msg.payload.data.decode())
                if recv_i - i > 1:
                    return False
                i = recv_i + 1
                await connector.send_message(Bytes(f"{i}".encode()))

                # when the connector sends `limit - 1`, we're done
                if i >= limit - 1:
                    break
            return True

        binder_success, connector_success = await asyncio.gather(
            binder_routine(binder, 100), connector_routine(connector, 100)
        )

        if not binder_success:
            self.fail("binder failed")

        if not connector_success:
            self.fail("connector failed")

    async def test_big_message(self):
        # Remove slow asyncio routine warnings
        loop = asyncio.get_event_loop()
        loop.slow_callback_duration = 0.5

        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")
        self.assertEqual(binder.identity, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector = ConnectorSocket.connect(ctx, "connector", repr(address))
        self.assertEqual(connector.identity, "connector")

        for _ in range(10):
            await connector.send_message(Bytes(b"." * 500_000_000))
            msg = await binder.recv_message()

            assert msg.address is not None
            self.assertEqual(msg.address.data, b"connector")
            self.assertEqual(msg.payload.data, b"." * 500_000_000)

    async def test_multicast(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector1 = ConnectorSocket.connect(ctx, "connector1", repr(address))
        connector2 = ConnectorSocket.connect(ctx, "connector2", repr(address))

        # make sure all connectors are connected

        await connector1.send_message(Bytes(b"ready1"))
        await connector2.send_message(Bytes(b"ready2"))

        await binder.recv_message()
        await binder.recv_message()

        # send a broadcast message

        binder.send_multicast_message(Bytes(b"all"))

        msg1 = await connector1.recv_message()
        msg2 = await connector2.recv_message()
        self.assertEqual(msg1.payload.data, b"all")
        self.assertEqual(msg2.payload.data, b"all")

        # send a multicast message only matching connector 1

        binder.send_multicast_message(Bytes(b"filtered"), "connector1")

        msg1 = await connector1.recv_message()
        self.assertEqual(msg1.payload.data, b"filtered")

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(connector2.recv_message(), timeout=0.2)  # connector2 should not receive anything

    async def test_tls(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")

        tls_config = TLSConfig(cert_chain=_CERT_CHAIN, private_key=_PRIVATE_KEY)

        # Only provide a certificate chain and private key to the binder (server).
        bound_address = await binder.bind_to("tls://127.0.0.1:0", tls_config)
        self.assertEqual(repr(bound_address)[:6], "tls://")

        connector = ConnectorSocket.connect(ctx, "connector", repr(bound_address))
        self.assertEqual(connector.identity, "connector")

        # Connector -> Binder
        await connector.send_message(Bytes(b"secret-payload"))
        msg = await asyncio.wait_for(binder.recv_message(), timeout=5.0)

        assert msg.address is not None
        self.assertEqual(msg.address.data, b"connector")
        self.assertEqual(msg.payload.data, b"secret-payload")

        # Binder -> Connector
        await binder.send_message("connector", Bytes(b"secret-reply"))
        msg = await asyncio.wait_for(connector.recv_message(), timeout=5.0)
        self.assertEqual(msg.payload.data, b"secret-reply")
