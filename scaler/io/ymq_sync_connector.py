import logging
import os
import socket
import threading
import uuid
import sys
from typing import Optional

from scaler.io.mixins import SyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.io.ymq import ymq
from scaler.protocol.python.mixins import Message
from scaler.utility.zmq_config import ZMQConfig

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = object


class ZMQSyncConnector(SyncConnector):
    def __init__(
        self, context: ymq.IOContext, socket_type: ymq.IOSocketType, address: ZMQConfig, identity: Optional[bytes]
    ):
        self._address = address

        self._context = context

        self._identity: bytes = (
            f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}".encode()
            if identity is None
            else identity
        )

        self._socket = self._context.createIOSocket_sync(self.identity.decode(), socket_type)
        self._socket.connect_sync(self._address.to_address())

        self._lock = threading.Lock()

    # TODO: this needs to interrupt the socket operations somehow
    # consider adding request stop to the interface?
    def destroy(self):
        self._context = None
        self._socket = None

    @property
    def address(self) -> str:
        return self._address.to_address()

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: Message):
        with self._lock:
            self._socket.send_sync(ymq.Message(address=None, payload=serialize(message)))

    def receive(self) -> Optional[Message]:
        with self._lock:
            msg = self._socket.recv_sync()

        return self.__compose_message(msg.payload)

    def __compose_message(self, payload: Buffer) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
