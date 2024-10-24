import logging
import os
import uuid
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Optional

import zmq.asyncio
from zmq import Frame

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.zmq_config import ZMQConfig


class AsyncBinder(Looper, Reporter):
    def __init__(self, context: zmq.asyncio.Context, name: str, address: ZMQConfig, identity: Optional[bytes] = None):
        self._address = address

        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()
        self._identity = identity

        self._context = context
        self._socket = self._context.socket(zmq.ROUTER)
        self.__set_socket_options()
        self._socket.bind(self._address.to_address())

        self._callback: Optional[Callable[[bytes, Message], Awaitable[None]]] = None

        self._received: Dict[str, int] = defaultdict(lambda: 0)
        self._sent: Dict[str, int] = defaultdict(lambda: 0)

    def destroy(self):
        self._context.destroy(linger=0)

    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        frames: List[Frame] = await self._socket.recv_multipart(copy=False)
        if not self.__is_valid_message(frames):
            return

        source, payload = frames
        message: Optional[Message] = deserialize(payload.bytes)
        if message is None:
            logging.error(f"received unknown message from {source.bytes!r}: {payload!r}")
            return

        self.__count_received(message.__class__.__name__)
        await self._callback(source.bytes, message)

    async def send(self, to: bytes, message: Message):
        self.__count_sent(message.__class__.__name__)
        await self._socket.send_multipart([to, serialize(message)], copy=False)

    def get_status(self) -> BinderStatus:
        return BinderStatus.new_msg(received=self._received, sent=self._sent)

    def __set_socket_options(self):
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

    def __is_valid_message(self, frames: List[Frame]) -> bool:
        if len(frames) < 2:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return False

        return True

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
