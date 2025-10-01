import logging
import os
import socket
import threading
import uuid
from typing import Optional

from scaler.io.ymq import ymq

from scaler.io.mixins import SyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.utility.zmq_config import ZMQConfig, ZMQType


class YMQSyncConnector(SyncConnector):
    def __init__(self, context: ymq.IOContext, socket_type: int, address: ZMQConfig, identity: Optional[bytes]):
        self._address = address
        self._context = context

        self._identity: bytes = (
            f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}".encode()
            if identity is None
            else identity
        )

        self._socket = self._context.createIOSocket_sync(self.identity.decode(), socket_type)

        if self._address.type != ZMQType.tcp:
            raise ValueError(f"YMQ only supports tcp transport, got {self._address.type}")

        self._socket.connect_sync(self._address.to_address())

        self._lock = threading.Lock()

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
            self._socket.send_sync(ymq.Message(None, serialize(message)), timeout_secs=3)

    def receive(self) -> Optional[Message]:
        print("NONONONONO")
        with self._lock:
            msg = self._socket.recv_sync(timeout_secs=3)

        # TODO: zero-copy
        return self.__compose_message(msg.payload.data)

    def __compose_message(self, payload: bytes) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
