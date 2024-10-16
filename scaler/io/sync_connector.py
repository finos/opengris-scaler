import logging
import os
import socket
import threading
import uuid
from typing import Optional

import zmq

from scaler.io.utility import deserialize, serialize
from scaler.protocol.python.mixins import Message
from scaler.utility.zmq_config import ZMQConfig


class SyncConnector:
    def __init__(self, context: zmq.Context, socket_type: int, address: ZMQConfig, identity: Optional[bytes]):
        self._address = address

        self._context = context
        self._socket = self._context.socket(socket_type)

        self._identity: bytes = (
            f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}".encode()
            if identity is None
            else identity
        )

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        self._socket.connect(self._address.to_address())

        self._lock = threading.Lock()

    def close(self):
        self._socket.close()

    @property
    def address(self) -> ZMQConfig:
        return self._address

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: Message):
        with self._lock:
            self._socket.send(serialize(message), copy=False)

    def receive(self) -> Optional[Message]:
        with self._lock:
            payload = self._socket.recv(copy=False)

        return self.__compose_message(payload.bytes)

    def __compose_message(self, payload: bytes) -> Optional[Message]:
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
