import logging
import os
import uuid
from typing import Awaitable, Callable, List, Literal, Optional

import zmq.asyncio

from scaler.protocol.python.message import PROTOCOL, MessageType, MessageVariant
from scaler.utility.zmq_config import ZMQConfig


class AsyncConnector:
    def __init__(
        self,
        context: zmq.asyncio.Context,
        name: str,
        socket_type: int,
        address: ZMQConfig,
        bind_or_connect: Literal["bind", "connect"],
        callback: Optional[Callable[[MessageType, MessageVariant], Awaitable[None]]],
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

        self._callback: Optional[Callable[[MessageVariant], Awaitable[None]]] = callback

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

        message = await self.receive()
        if message is None:
            return

        await self._callback(message)

    async def receive(self) -> Optional[MessageVariant]:
        if self._context.closed:
            return None

        if self._socket.closed:
            return None

        frames = await self._socket.recv_multipart()
        if not self.__is_valid_message(frames):
            return None

        message_type_bytes, *payload = frames
        message_type = MessageType(message_type_bytes)
        message = PROTOCOL[message_type].deserialize(payload)
        return message

    async def send(self, data: MessageVariant):
        message_type = PROTOCOL.inverse[type(data)]
        await self._socket.send_multipart([message_type.value, *data.serialize()], copy=False)

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 2:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected message type: {frames[0]}: {frames}")
            return False

        return True

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
