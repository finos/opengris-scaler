import logging
import os
import uuid
from typing import Awaitable, Callable, Literal, Optional

from scaler.config.types.zmq import ZMQConfig
from scaler.io.mixins import AsyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.io.uv_ymq import Bytes, ConnectorSocket, IOContext
from scaler.protocol.python.mixins import Message


class UVYMQAsyncConnector(AsyncConnector):
    def __init__(
        self,
        name: str,
        socket_type: int,
        address: ZMQConfig,
        bind_or_connect: Literal["bind", "connect"],
        callback: Optional[Callable[[Message], Awaitable[None]]],
        identity: Optional[bytes],
    ):
        self._address = address

        self._context = IOContext()

        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4().bytes.hex()}".encode()
        self._identity = identity

        self._callback: Optional[Callable[[Message], Awaitable[None]]] = callback

        # Create connector socket
        if bind_or_connect == "bind":
            self._socket = ConnectorSocket.bind(self._context, self._identity.decode(), self.address)
        elif bind_or_connect == "connect":
            self._socket = ConnectorSocket.connect(self._context, self._identity.decode(), self.address)
        else:
            raise TypeError("bind_or_connect has to be 'bind' or 'connect'")

    def destroy(self):
        self._socket = None
        self._context = None

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def socket(self) -> ConnectorSocket:
        return self._socket

    @property
    def address(self) -> str:
        return self._address.to_address()

    async def routine(self):
        if self._callback is None:
            return

        message: Optional[Message] = await self.receive()
        if message is None:
            return

        await self._callback(message)

    async def receive(self) -> Optional[Message]:
        if self._context is None:
            return None

        if self._socket is None:
            return None

        msg = await self._socket.recv_message()
        result: Optional[Message] = deserialize(msg.payload.data)
        if result is None:
            logging.error(f"received unknown message: {msg.payload.data!r}")
            return None

        return result

    async def send(self, message: Message):
        await self._socket.send_message(Bytes(serialize(message)))
