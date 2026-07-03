import asyncio
import pathlib
import unittest

from scaler.io.ymq import (
    BinderSocket,
    Bytes,
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

        binder_success, connector_success = await asyncio.wait_for(
            asyncio.gather(binder_routine(binder, 100), connector_routine(connector, 100)), timeout=30.0
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

        # A few MB round-trips fine (was 500MB x10, trimmed for CI speed); hoist the literal to avoid
        # re-allocating it each iteration.
        expected_payload = b"." * 4_000_000
        for _ in range(3):
            await connector.send_message(Bytes(expected_payload))
            msg = await binder.recv_message()

            assert msg.address is not None
            self.assertEqual(msg.address.data, b"connector")
            self.assertEqual(msg.payload.data, expected_payload)

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
