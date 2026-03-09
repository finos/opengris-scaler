import logging
import os
import uuid
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Optional

from scaler.config.types.zmq import ZMQConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.utility import deserialize, serialize
from scaler.io.uv_ymq import BinderSocket, Bytes, IOContext
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus


class UVYMQAsyncBinder(AsyncBinder):
    def __init__(self, name: str, address: ZMQConfig, identity: Optional[bytes] = None):
        self._address = address

        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()
        self._identity = identity

        self._context = IOContext()

        self._socket = BinderSocket(self._context, self._identity.decode())
        self._socket.bind_to_sync(self._address.to_address())

        self._callback: Optional[Callable[[bytes, Message], Awaitable[None]]] = None

        self._received: Dict[str, int] = defaultdict(lambda: 0)
        self._sent: Dict[str, int] = defaultdict(lambda: 0)

    @property
    def identity(self):
        return self._identity

    def destroy(self):
        self._socket = None
        self._context = None

    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        uv_ymq_msg = await self._socket.recv_message()

        message: Optional[Message] = deserialize(uv_ymq_msg.payload.data)
        if message is None:
            logging.error(f"received unknown message from {uv_ymq_msg.address.data!r}: {uv_ymq_msg.payload.data!r}")
            return

        self.__count_received(message.__class__.__name__)
        await self._callback(uv_ymq_msg.address.data, message)

    async def send(self, to: bytes, message: Message):
        self.__count_sent(message.__class__.__name__)
        await self._socket.send_message(to.decode(), Bytes(serialize(message)))

    def get_status(self) -> BinderStatus:
        return BinderStatus.new_msg(received=self._received, sent=self._sent)

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1
