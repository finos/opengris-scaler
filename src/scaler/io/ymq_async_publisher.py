from typing import Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncPublisher
from scaler.io.utility import serialize
from scaler.io.ymq import BinderSocket, Bytes, IOContext
from scaler.protocol.capnp import BaseMessage


class YMQAsyncPublisher(AsyncPublisher):
    def __init__(self, context: IOContext, identity: bytes):
        self._ymq_context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None
        self._socket: Optional[BinderSocket] = BinderSocket(self._ymq_context, self._identity.decode())

    async def bind(self, address: AddressConfig) -> None:
        assert self._socket is not None

        bound_address = await self._socket.bind_to(repr(address))
        self._address = AddressConfig.from_string(repr(bound_address))

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket is None:
            return

        self._socket.shutdown()

        self._socket = None
        self._ymq_context = None

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def send(self, message: BaseMessage):
        if self._socket is None:
            return

        self._socket.send_multicast_message(Bytes(serialize(message)))
