import logging
from typing import Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import SyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import Bytes, ConnectorSocket, IOContext
from scaler.protocol.capnp import BaseMessage


class YMQSyncConnector(SyncConnector):
    def __init__(self, context: IOContext, identity: bytes, address: AddressConfig):
        self._context = context
        self._identity = identity
        self._address = address

        self._socket: Optional[ConnectorSocket] = ConnectorSocket.connect(
            self._context, self._identity.decode(), repr(self._address)
        )

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket is None:
            return

        self._socket.shutdown()

        self._socket = None
        self._context = None

    @property
    def address(self) -> AddressConfig:
        return self._address

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: BaseMessage):
        if self._socket is None:
            return

        self._socket.send_message_sync(Bytes(serialize(message)))

    def receive(self) -> Optional[BaseMessage]:
        if self._socket is None:
            return None

        payload_data = self._socket.recv_message_sync().payload.data

        if payload_data is None:
            return None

        return self.__compose_message(payload_data)

    def __compose_message(self, payload: bytes) -> Optional[BaseMessage]:
        result: Optional[BaseMessage] = deserialize(payload)
        if result is None:
            logging.error(f"received unknown message: {payload!r}")
            return None

        return result
