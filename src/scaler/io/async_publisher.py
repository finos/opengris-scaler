from typing import Optional

import zmq
import zmq.asyncio

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncPublisher
from scaler.io.utility import serialize
from scaler.protocol.capnp import BaseMessage


class ZMQAsyncPublisher(AsyncPublisher):
    def __init__(self, context: zmq.asyncio.Context, identity: bytes):
        self._context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None
        self._socket: Optional[zmq.asyncio.Socket] = None

    async def bind(self, address: AddressConfig) -> None:
        assert self._socket is None

        self._socket = self._context.socket(zmq.PUB)
        assert self._socket is not None

        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.bind(repr(address))

        endpoint = self._socket.getsockopt(zmq.LAST_ENDPOINT)
        assert isinstance(endpoint, bytes)
        self._address = AddressConfig.from_string(endpoint.decode())

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket is None:
            return

        if self._socket.closed:
            return

        self._socket.close(linger=1)

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def send(self, message: BaseMessage):
        if self._socket is None:
            return

        await self._socket.send(serialize(message), copy=False)
