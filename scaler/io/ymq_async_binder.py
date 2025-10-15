import logging
import os
import uuid
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Optional


from scaler.io.mixins import AsyncBinder
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import ymq
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus
from scaler.config.types.zmq import ZMQConfig


class YMQAsyncBinder(AsyncBinder):
    def __init__(self, context: ymq.IOContext, name: str, address: ZMQConfig, identity: Optional[bytes] = None):
        self._address = address

        if identity is None:
            identity = f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()
        self._identity = identity

        self._context = context
        self._socket = self._context.createIOSocket_sync(self.identity.decode(), ymq.IOSocketType.Binder)
        self._socket.bind_sync(self._address.to_address())

        self._callback: Optional[Callable[[bytes, Message], Awaitable[None]]] = None

        self._received: Dict[str, int] = defaultdict(lambda: 0)
        self._sent: Dict[str, int] = defaultdict(lambda: 0)

    @property
    def identity(self):
        return self._identity

    def destroy(self):
        self._context = None
        self._socket = None

    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        ymqmsg = await self._socket.recv()

        message: Optional[Message] = deserialize(ymqmsg.payload.data)
        if message is None:
            logging.error(f"received unknown message from {ymqmsg.address.data!r}: {ymqmsg.address.data!r}")
            return

        self.__count_received(message.__class__.__name__)
        await self._callback(ymqmsg.address.data, message)

    async def send(self, to: bytes, message: Message):
        self.__count_sent(message.__class__.__name__)
        await self._socket.send(ymq.Message(address=to, payload=serialize(message)))

    def get_status(self) -> BinderStatus:
        return BinderStatus.new_msg(received=self._received, sent=self._sent)

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1
