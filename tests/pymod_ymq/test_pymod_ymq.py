import asyncio
import multiprocessing
import multiprocessing.connection
import unittest
from scaler.io.ymq import ymq
from scaler.io.utility import serialize, deserialize
from scaler.protocol.python.message import TaskCancel
from scaler.utility.identifiers import TaskID


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

    @unittest.skip("this test currently hangs, see comment in the code")
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
                    # TODO: change to `asyncio.timeout()` in python >3.10
                    await asyncio.wait_for(binder.send(ymq.Message(address=None, payload=b"payload")), 30)

                    # TODO: solve the hang and write the rest of the test
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

        if not result:
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

    async def test_buffer_interface(self):
        msg = TaskCancel.new_msg(TaskID.generate_task_id())
        data = serialize(msg)

        # verify that capnp can deserialize this data
        _ = deserialize(data)

        # this creates a copy of the data
        copy = ymq.Bytes(data)

        # this should deserialize without creating a copy
        # because ymq.Bytes uses the buffer protocol
        deserialized: TaskCancel = deserialize(copy)
        self.assertEqual(deserialized.task_id, msg.task_id)
