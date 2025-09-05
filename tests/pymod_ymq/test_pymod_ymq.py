import multiprocessing.connection
import unittest
from .config import ymq
import asyncio
import multiprocessing


class TestPymodYMQ(unittest.IsolatedAsyncioTestCase):
    async def test_basic(self):
        ctx = ymq.IOContext()
        binder = await ctx.createIOSocket("binder", ymq.IOSocketType.Binder)
        self.assertEqual(binder.identity, "binder")
        self.assertEqual(binder.socket_type, ymq.IOSocketType.Binder)

        connector = await ctx.createIOSocket("connector", ymq.IOSocketType.Connector)
        self.assertEqual(connector.identity, "connector")
        self.assertEqual(connector.socket_type, ymq.IOSocketType.Connector)

        await binder.bind("tcp://127.0.0.1:35791")
        await connector.connect("tcp://127.0.0.1:35791")

        await connector.send(ymq.Message(address=None, payload=b"payload"))
        msg = await binder.recv()

        assert msg.address is not None
        self.assertEqual(msg.address.data, b"connector")
        self.assertEqual(msg.payload.data, b"payload")

    async def test_no_address(self):
        # this test requires special care because it hangs and doesn't shut down the worker threads properly
        # we use a subprocess to shield us from any effects
        pipe_parent, pipe_child = multiprocessing.Pipe(duplex=False)

        def test(pipe: multiprocessing.connection.Connection) -> None:
            async def main():
                ctx = ymq.IOContext()
                binder = await ctx.createIOSocket("binder", ymq.IOSocketType.Binder)
                connector = await ctx.createIOSocket("connector", ymq.IOSocketType.Connector)

                await binder.bind("tcp://127.0.0.1:35791")
                await connector.connect("tcp://127.0.0.1:35791")

                try:
                    # note: change to `asyncio.timeout()` in python >3.10
                    await asyncio.wait_for(binder.send(ymq.Message(address=None, payload=b"payload")), 2)

                    # TODO: solve the hang and write the rest of the test?
                    pipe.send(True)
                except asyncio.TimeoutError:
                    pipe.send(False)

            asyncio.run(main())

        p = multiprocessing.Process(target=test, args=(pipe_child,))
        p.start()
        result = pipe_parent.recv()
        p.join(5)
        if p.exitcode is None:
            p.kill()

        # TODO: fix this so it dosn't hang and change this to `not result`?
        if result:
            self.fail()

    async def test_routing(self):
        ctx = ymq.IOContext()
        binder = await ctx.createIOSocket("binder", ymq.IOSocketType.Binder)
        connector1 = await ctx.createIOSocket("connector1", ymq.IOSocketType.Connector)
        connector2 = await ctx.createIOSocket("connector2", ymq.IOSocketType.Connector)

        await binder.bind("tcp://127.0.0.1:35791")
        await connector1.connect("tcp://127.0.0.1:35791")
        await connector2.connect("tcp://127.0.0.1:35791")

        await binder.send(ymq.Message(b"connector2", b"2"))
        await binder.send(ymq.Message(b"connector1", b"1"))

        msg1 = await connector1.recv()
        self.assertEqual(msg1.payload.data, b"1")

        msg2 = await connector2.recv()
        self.assertEqual(msg2.payload.data, b"2")

    async def test_pingpong(self):
        ctx = ymq.IOContext()
        binder = await ctx.createIOSocket("binder", ymq.IOSocketType.Binder)
        connector = await ctx.createIOSocket("connector", ymq.IOSocketType.Connector)

        await binder.bind("tcp://127.0.0.1:35791")
        await connector.connect("tcp://127.0.0.1:35791")

        async def binder_routine(binder: ymq.IOSocket, limit: int) -> bool:
            i = 0
            while i < limit:
                await binder.send(ymq.Message(address=b"connector", payload=f"{i}".encode()))
                msg = await binder.recv()
                assert msg.payload.data is not None

                recv_i = int(msg.payload.data.decode())
                if recv_i - i > 1:
                    print(f"{recv_i}, {i}")
                    return False
                i = recv_i + 1
            return True

        async def connector_routine(connector: ymq.IOSocket, limit: int) -> bool:
            i = 0
            while True:
                msg = await connector.recv()
                assert msg.payload.data is not None
                recv_i = int(msg.payload.data.decode())
                if recv_i - i > 1:
                    return False
                i = recv_i + 1
                await connector.send(ymq.Message(address=None, payload=f"{i}".encode()))

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
        ctx = ymq.IOContext()
        binder = await ctx.createIOSocket("binder", ymq.IOSocketType.Binder)
        self.assertEqual(binder.identity, "binder")
        self.assertEqual(binder.socket_type, ymq.IOSocketType.Binder)

        connector = await ctx.createIOSocket("connector", ymq.IOSocketType.Connector)
        self.assertEqual(connector.identity, "connector")
        self.assertEqual(connector.socket_type, ymq.IOSocketType.Connector)

        await binder.bind("tcp://127.0.0.1:35791")
        await connector.connect("tcp://127.0.0.1:35791")

        for _ in range(10):
            await connector.send(ymq.Message(address=None, payload=b"." * 500_000_000))
            msg = await binder.recv()

            assert msg.address is not None
            self.assertEqual(msg.address.data, b"connector")
            self.assertEqual(msg.payload.data, b"." * 500_000_000)
