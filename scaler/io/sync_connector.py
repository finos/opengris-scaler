import logging
import os
import socket
import threading
import uuid
from typing import List, Optional

import zmq

from scaler.protocol.python.message import PROTOCOL, MessageType, MessageVariant
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

    def send(self, message: MessageVariant):
        message_type = PROTOCOL.inverse[type(message)]

        with self._lock:
            self._socket.send_multipart([message_type.value, *message.serialize()])

    def receive(self) -> Optional[MessageVariant]:
        with self._lock:
            frames = self._socket.recv_multipart()

        return self.__compose_message(frames)

    def __compose_message(self, frames: List[bytes]) -> Optional[MessageVariant]:
        if len(frames) < 2:
            logging.error(f"{self.__get_prefix()} received unexpected frames {frames}")
            return None

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"{self.__get_prefix()} received unexpected message type: {frames[0]}: {frames}")
            return None

        message_type_bytes, *payload = frames
        message_type = MessageType(message_type_bytes)
        message = PROTOCOL[message_type].deserialize(payload)
        return message

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity.decode()}]:"
