import logging
from collections import defaultdict
from typing import Awaitable, Callable, Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import BinderSocket, Bytes, ConnectorSocketClosedByRemoteEndError, IOContext
from scaler.protocol.capnp import BaseMessage, BinderStatus


class YMQAsyncBinder(AsyncBinder):
    def __init__(self, context: IOContext, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]):
        self._ymq_context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None

        self._socket: Optional[BinderSocket] = BinderSocket(self._ymq_context, self._identity.decode())

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
        self._ymq_context = None

    async def routine(self):
        assert self._socket is not None
        ymq_msg = await self._socket.recv_message()

        message: Optional[BaseMessage] = deserialize(ymq_msg.payload.data)
        if message is None:
            logging.error(f"received unknown message from {ymq_msg.address.data!r}: {ymq_msg.payload.data!r}")
            return

        self.__count_received(message.__class__.__name__)
        try:
            await self._callback(ymq_msg.address.data, message)
        except ConnectorSocketClosedByRemoteEndError:
            # The callback (e.g. on_heartbeat -> binder.send echo) tried to reply to a peer that
            # had already disconnected by the time Python caught up on libuv-buffered messages.
            # Treat as a no-op: peer-gone is a routine event handled by the scheduler controllers'
            # own timeout/cleanup paths. Re-raising would bubble up through asyncio.gather and
            # tear down the whole scheduler for what is a normal peer departure.
            pass

    async def send(self, to: bytes, message: BaseMessage):
        # Errors (including ConnectorSocketClosedByRemoteEndError when the peer is gone) propagate
        # up to whoever drove this send - the AsyncBinder send/recv API maps 1:1 to the underlying
        # C++ BinderSocket. The scheduler-side swallow lives in routine() above, which is the loop
        # that actually has to stay alive across peer departures.
        assert self._socket is not None
        self.__count_sent(message.__class__.__name__)
        await self._socket.send_message(to.decode(), Bytes(serialize(message)))

    def get_status(self) -> BinderStatus:
        return BinderStatus(
            received=[
                BinderStatus.Pair(client=message_type, number=count) for message_type, count in self._received.items()
            ],
            sent=[BinderStatus.Pair(client=message_type, number=count) for message_type, count in self._sent.items()],
        )

    def __count_received(self, message_type: str):
        self._received[message_type] += 1

    def __count_sent(self, message_type: str):
        self._sent[message_type] += 1
