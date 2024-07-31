import logging
import os
import uuid
from collections import defaultdict
from typing import Awaitable, Callable, List, Literal, Optional

import zmq.asyncio

from scaler.protocol.python.message import PROTOCOL, MessageType, MessageVariant
from scaler.protocol.python.status import BinderStatus
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.zmq_config import ZMQConfig


class AsyncBinder(Looper, Reporter):
    def __init__(self, name: str, address: ZMQConfig, io_threads: int, identity: Optional[bytes] = None):
        self._address = address

        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()
        self._identity = identity

        self._context = zmq.asyncio.Context(io_threads=io_threads)
        self._socket = self._context.socket(zmq.ROUTER)
        self.__set_socket_options()
        self._socket.bind(self._address.to_address())

        self._callback: Optional[Callable[[bytes, MessageVariant], Awaitable[None]]] = None

        self._statistics = {"received": defaultdict(lambda: 0), "sent": defaultdict(lambda: 0)}

    def destroy(self):
        self._context.destroy(linger=0)

    def register(self, callback: Callable[[bytes, MessageVariant], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        frames = await self._socket.recv_multipart()
        if not self.__is_valid_message(frames):
            return

        source, message_type_bytes, payload = frames[0], frames[1], frames[2:]
        message_type = MessageType(message_type_bytes)
        self.__count_one("received", message_type)

        message = PROTOCOL[message_type].deserialize(payload)
        await self._callback(source, message)

    async def send(self, to: bytes, message: MessageVariant):
        message_type = PROTOCOL.inverse[type(message)]
        self.__count_one("sent", message_type)
        await self._socket.send_multipart([to, message_type.value, *message.serialize()], copy=False)

    def get_status(self) -> BinderStatus:
        return BinderStatus(
            received={k: v for k, v in self._statistics["received"].items()},
            sent={k: v for k, v in self._statistics["sent"].items()},
        )

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __is_valid_message(self, frames: List[bytes]) -> bool:
        if len(frames) < 3:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        if frames[1] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected message type: {frames[0]}: {frames}")
            return False

        return True

    def __count_one(self, count_type: Literal["sent", "received"], message_type: MessageType):
        self._statistics[count_type][message_type.name] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
