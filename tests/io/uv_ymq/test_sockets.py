import asyncio
import unittest

from scaler.io.uv_ymq import (
    BinderSocket,
    Bytes,
    ConnectorSocket,
    ErrorCode,
    IOContext,
)
from scaler.io.uv_ymq import _uv_ymq


class TestSockets(unittest.IsolatedAsyncioTestCase):
    async def test_basic(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")
        self.assertEqual(binder.identity, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector = ConnectorSocket(ctx, "connector", repr(address))
        self.assertEqual(connector.identity, "connector")

        await connector.send_message(Bytes(b"payload"))
        msg = await binder.recv_message()

        assert msg.address is not None
        self.assertEqual(msg.address.data, b"connector")
        self.assertEqual(msg.payload.data, b"payload")

    async def test_invalid_address(self):
        ctx = IOContext()

        with self.assertRaises(_uv_ymq.InvalidAddressFormatError) as exc:
            await BinderSocket(ctx, "binder").bind_to("invalid_address")
        self.assertEqual(exc.exception.code, ErrorCode.InvalidAddressFormat)

    async def test_routing(self):
        ctx = IOContext()
        binder = BinderSocket(ctx, "binder")

        address = await binder.bind_to("tcp://127.0.0.1:0")

        connector1 = ConnectorSocket(ctx, "connector1", repr(address))
        connector2 = ConnectorSocket(ctx, "connector2", repr(address))

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

        connector = ConnectorSocket(ctx, "connector", repr(address))

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

        connector = ConnectorSocket(ctx, "connector", repr(address))
        self.assertEqual(connector.identity, "connector")

        for _ in range(10):
            await connector.send_message(Bytes(b"." * 500_000_000))
            msg = await binder.recv_message()

            assert msg.address is not None
            self.assertEqual(msg.address.data, b"connector")
            self.assertEqual(msg.payload.data, b"." * 500_000_000)
