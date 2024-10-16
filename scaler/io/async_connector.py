import logging
import os
import uuid
from typing import Awaitable, Callable, List, Literal, Optional

import zmq.asyncio

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.utility.zmq_config import ZMQConfig


class AsyncConnector:
    def __init__(
        self,
        context: zmq.asyncio.Context,
        name: str,
        socket_type: int,
        address: ZMQConfig,
        bind_or_connect: Literal["bind", "connect"],
        callback: Optional[Callable[[Message], Awaitable[None]]],
        identity: Optional[bytes],
    ):
        self._address = address

        self._context = context
        self._socket = self._context.socket(socket_type)

        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4().bytes.hex()}".encode()
        self._identity = identity

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        if bind_or_connect == "bind":
            self._socket.bind(self._address.to_address())
        elif bind_or_connect == "connect":
            self._socket.connect(self._address.to_address())
        else:
            raise TypeError("bind_or_connect has to be 'bind' or 'connect'")

        self._callback: Optional[Callable[[Message], Awaitable[None]]] = callback

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket.closed:
            return

        self._socket.close(linger=1)

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def socket(self) -> zmq.asyncio.Socket:
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
        if self._context.closed:
            return None

        if self._socket.closed:
            return None

        payload = await self._socket.recv(copy=False)
        result: Optional[Message] = deserialize(payload.bytes)
        if result is None:
            logging.error(f"received unknown message: {payload.bytes!r}")
            return None

        return result

    async def send(self, message: Message):
        await self._socket.send(serialize(message), copy=False)

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) > 1:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        return True

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
