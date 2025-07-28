import logging
from typing import Optional

import sys
if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = object

from scaler.io.ymq.ymq import IOContext, IOSocketType
from scaler.io.ymq import ymq

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.utility.identifiers import ClientID, Identifier
from scaler.utility.ymq_config import YMQConfig


class SyncConnector:
    def __init__(self, context: IOContext, socket_type: IOSocketType, address: YMQConfig, identity: Optional[Identifier]):
        identity = identity or ClientID.generate_client_id()

        self._identity = identity
        self._address = address
        self._context = context
        self._socket = self._context.createIOSocket_sync(identity.decode(), socket_type)

        self._socket.connect_sync(self._address.to_address())

    def close(self):
        pass

    @property
    def address(self) -> YMQConfig:
        return self._address

    @property
    def identity(self) -> Identifier | str:
        return self._identity

    def send(self, message: Message):
        self._socket.send_sync(ymq.Message(address=None, payload=serialize(message)))

    def receive(self) -> Optional[Message]:
        message: ymq.Message = self._socket.recv_sync()

        return self.__compose_message(message.payload)

    def __compose_message(self, payload: Buffer) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity}]:"
