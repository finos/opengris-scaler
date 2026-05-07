import logging
import threading
from typing import Optional

import zmq

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import ConnectorRemoteType, SyncConnector
from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import BaseMessage


class ZMQSyncConnector(SyncConnector):
    def __init__(
        self, context: zmq.Context, identity: bytes, address: AddressConfig, connector_remote_type: ConnectorRemoteType
    ):
        self._context = context
        self._identity = identity
        self._address = address

        self._socket = self._context.socket(self.__to_zmq_socket_type(connector_remote_type))

        # set socket option
        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        self._socket.connect(repr(self._address))

        self._lock = threading.Lock()

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket.closed:
            return

        self._socket.close(linger=1)

    @property
    def address(self) -> AddressConfig:
        return self._address

    @property
    def identity(self) -> bytes:
        return self._identity

    def send(self, message: BaseMessage):
        with self._lock:
            self._socket.send(serialize(message), copy=False)

    def receive(self) -> Optional[BaseMessage]:
        with self._lock:
            payload = self._socket.recv(copy=False)

        return self.__compose_message(payload.bytes)

    def __compose_message(self, payload: bytes) -> Optional[BaseMessage]:
        result: Optional[BaseMessage] = deserialize(payload)
        if result is None:
            logging.error(f"{self.__get_prefix()}: received unknown message: {payload!r}")
            return None

        return result

    def __get_prefix(self):
        return f"{self.__class__.__name__}[{self._identity!r}]:"

    @staticmethod
    def __to_zmq_socket_type(connector_remote_type: ConnectorRemoteType) -> int:
        if connector_remote_type == ConnectorRemoteType.Connector:
            return zmq.PAIR

        if connector_remote_type == ConnectorRemoteType.Binder:
            return zmq.DEALER

        raise ValueError(f"unsupported connector_remote_type={connector_remote_type}")
