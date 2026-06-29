"""io-layer contract tests for ``YMQAsyncBinder``.

These document the origin of the ``jne-fix-ymq`` failure: a binder send surfaces
``SocketStopRequested`` when its own socket is shut down (``disconnect``/teardown) while a send is
in flight. The io layer deliberately does NOT swallow this in ``send()`` - it fails fast so the
error is visible during development. The graceful, "log not crash" handling lives at the worker's
loop boundary; see ``tests/worker/test_worker.py``.
"""

import asyncio
import unittest
from typing import List, Tuple

from scaler.config.types.address import AddressConfig
from scaler.io.utility import deserialize
from scaler.io.ymq import ConnectorSocket, IOContext, SocketStopRequestedError
from scaler.io.ymq_async_binder import YMQAsyncBinder
from scaler.protocol.capnp import BaseMessage, DisconnectRequest
from scaler.utility.identifiers import WorkerID


class TestYMQAsyncBinderSend(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self._received: List[Tuple[bytes, BaseMessage]] = []

        self._context = IOContext()
        self._binder = YMQAsyncBinder(self._context, identity=b"binder-under-test", callback=self._on_receive)
        await self._binder.bind(AddressConfig.from_string("tcp://127.0.0.1:0"))

    async def _on_receive(self, address: bytes, message: BaseMessage) -> None:
        self._received.append((address, message))

    @staticmethod
    def _make_message() -> DisconnectRequest:
        return DisconnectRequest(worker=WorkerID.generate_worker_id("nobody"))

    async def test_send_propagates_socket_stop_requested_when_socket_shut_down(self) -> None:
        """binder.send surfaces SocketStopRequested when its own socket is shut down mid-send.

        The send is queued inside the C++ binder (the peer never connects), then the binder is
        destroyed. The native socket fails the pending send with ``SocketStopRequested``, which the
        io layer propagates as-is (fail fast). This is the exception the worker boundary must handle.
        """
        send_task = asyncio.ensure_future(self._binder.send(b"peer-that-never-connects", self._make_message()))

        # Let the send reach the binder's event-loop thread and park in its pending-send queue.
        await asyncio.sleep(0.2)
        self.assertFalse(send_task.done(), "send should still be pending (peer never connected)")

        # Shut the binder down while the send is in flight (mirrors worker teardown / `disconnect`).
        self._binder.destroy()

        with self.assertRaises(SocketStopRequestedError):
            await asyncio.wait_for(send_task, timeout=5.0)

    async def test_normal_send_still_delivers(self) -> None:
        """A normal send still reaches a connected peer (happy path is unaffected)."""
        connector = ConnectorSocket.connect(self._context, "peer", repr(self._binder.address))

        message = self._make_message()
        await self._binder.send(b"peer", message)  # completes once the peer identifies itself

        ymq_msg = await asyncio.wait_for(connector.recv_message(), timeout=5.0)
        received = deserialize(ymq_msg.payload.data)

        assert isinstance(received, DisconnectRequest)
        self.assertEqual(received.worker, message.worker)


if __name__ == "__main__":
    unittest.main()
