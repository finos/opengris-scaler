import logging
from typing import Awaitable, Callable, Literal, Optional

from scaler.io.ymq.ymq import IOContext, IOSocket, IOSocketType
from scaler.io.ymq import ymq

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.utility.identifiers import ClientID, Identifier
from scaler.utility.ymq_config import YMQConfig


class AsyncConnector:
    def __init__(
        self,
        context: IOContext,
        name: str,
        address: YMQConfig,
        callback: Optional[Callable[[Message], Awaitable[None]]],
        identity: Optional[Identifier],
    ):
        self._address = address
        self._context = context
        if identity is None:
            identity = ClientID.generate_client_id(name)
        self._identity = identity
        self._callback: Optional[Callable[[Message], Awaitable[None]]] = callback

    def init_sync(self, bind_or_connect: Literal["bind", "connect"], socket_type: IOSocketType):
        self._socket = self._context.createIOSocket_sync(self._identity.decode(), socket_type)
        if bind_or_connect == "bind":
            self._socket.bind_sync(self._address.to_address())
        elif bind_or_connect == "connect":
            self._socket.connect_sync(self._address.to_address())
        else:
            raise TypeError("bind_or_connect has to be 'bind' or 'connect'")

    async def init(self, bind_or_connect: Literal["bind", "connect"], socket_type: IOSocketType):
        self._socket = await self._context.createIOSocket(self._identity.decode(), socket_type)
        if bind_or_connect == "bind":
            await self._socket.bind(self._address.to_address())
        elif bind_or_connect == "connect":
            await self._socket.connect(self._address.to_address())
        else:
            raise TypeError("bind_or_connect has to be 'bind' or 'connect'")

    def __del__(self):
        self.destroy()

    def destroy(self):
        pass

    @property
    def identity(self) -> Identifier:
        return self._identity

    @property
    def socket(self) -> IOSocket:
        return self._socket

    @property
    def address(self) -> YMQConfig:
        return self._address

    async def routine(self):
        if self._callback is None:
            return

        message: Optional[Message] = await self.receive()
        if message is None:
            return

        await self._callback(message)

    async def receive(self) -> Optional[Message]:
        message: ymq.Message = await self._socket.recv()

        # TODO: zero-copy
        deserialized: Optional[Message] = deserialize(message.payload.data)
        if deserialized is None:
            logging.error(f"received unknown message: {message.payload.data!r}")
            return None

        return deserialized

    async def send(self, message: Message):
        await self._socket.send(ymq.Message(address=None, payload=serialize(message)))
