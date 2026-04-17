import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import BinderSocket, Bytes, IOContext
from scaler.protocol.capnp import BaseMessage, BinderStatus


class YMQAsyncBinder(AsyncBinder):
    def __init__(self, context: IOContext, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]):
        self._context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._socket: Optional[BinderSocket] = BinderSocket(self._context, self._identity.decode())

        self._callback: Callable[[bytes, BaseMessage], Awaitable[None]] = callback

        self._received: Dict[str, int] = defaultdict(lambda: 0)
        self._sent: Dict[str, int] = defaultdict(lambda: 0)

    def __del__(self):
        self.destroy()

    async def bind(self, address: AddressConfig) -> None:
        assert self._socket is not None
        bound_address = await self._socket.bind_to(repr(address))
        self._address = AddressConfig.from_string(repr(bound_address))

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    def destroy(self):
        if self._socket is None:
            return

        self._socket.shutdown()

        self._socket = None
        self._context = None

    async def routine(self):
        assert self._socket is not None
        ymq_msg = await self._socket.recv_message()

        message: Optional[BaseMessage] = deserialize(ymq_msg.payload.data)
        if message is None:
            logging.error(f"received unknown message from {ymq_msg.address.data!r}: {ymq_msg.payload.data!r}")
            return

        self.__count_received(message.__class__.__name__)
        await self._callback(ymq_msg.address.data, message)

    async def send(self, to: bytes, message: BaseMessage):
        assert self._socket is not None
        self.__count_sent(message.__class__.__name__)
        await self._socket.send_message(to.decode(), Bytes(serialize(message)))

    def get_status(self) -> BinderStatus:
        return BinderStatus(received=self._received, sent=self._sent)

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1
