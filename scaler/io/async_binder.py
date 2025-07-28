import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Optional

from scaler.io.ymq.ymq import IOContext, IOSocketType
from scaler.io.ymq import ymq

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus
from scaler.utility.mixins import Looper, Reporter
from scaler.utility.identifiers import ClientID, Identifier
from scaler.utility.ymq_config import YMQConfig


class AsyncBinder(Looper, Reporter):
    def __init__(self, context: IOContext, name: str, address: YMQConfig, identity: Optional[Identifier] = None):
        self._address = address

        if identity is None:
            identity = ClientID.generate_client_id(name)
        self._identity = identity

        self._context = context

        self._callback: Optional[Callable[[bytes, Message], Awaitable[None]]] = None

        self._received: Dict[str, int] = defaultdict(lambda: 0)
        self._sent: Dict[str, int] = defaultdict(lambda: 0)

    @property
    def identity(self):
        return self._identity

    async def init(self):
        self._socket = await self._context.createIOSocket(self.identity.decode(), IOSocketType.Binder)
        await self._socket.bind(self._address.to_address())

    def init_sync(self):
        self._socket = self._context.createIOSocket_sync(self.identity.decode(), IOSocketType.Binder)
        self._socket.bind_sync(self._address.to_address())

    def destroy(self):
        pass

    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        self._callback = callback

    async def routine(self):
        message: ymq.Message = await self._socket.recv()
        deseralized: Optional[Message] = deserialize(message.payload)

        if deseralized is None:
            logging.error(f"received unknown message from {message.address.data!r}: {message.payload.data!r}")
            return

        self.__count_received(message.__class__.__name__)

        if self._callback is None:
            raise RuntimeError(f"{self.__get_prefix()}: no callback registered")

        await self._callback(message.address.data, deseralized)

    async def send(self, to: bytes, message: Message):
        self.__count_sent(message.__class__.__name__)
        await self._socket.send(ymq.Message(to, serialize(message)))

    def get_status(self) -> BinderStatus:
        return BinderStatus.new_msg(received=self._received, sent=self._sent)

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity}]:"
