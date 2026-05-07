import logging
import threading
from datetime import timedelta
from typing import Callable, Optional

import zmq

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import SyncSubscriber
from scaler.io.utility import deserialize
from scaler.protocol.capnp import BaseMessage


class ZMQSyncSubscriber(SyncSubscriber):
    def __init__(
        self,
        context: zmq.Context,
        identity: bytes,
        address: AddressConfig,
        callback: Callable[[BaseMessage], None],
        timeout: Optional[timedelta] = None,
    ):
        super().__init__()

        self._stop_event = threading.Event()

        self._context: zmq.Context = context
        self._identity = identity
        self._address = address
        self._callback = callback
        self._timeout = timeout

        self.__initialize()

    def __close(self):
        self._socket.close()

    def __stop_polling(self):
        self._stop_event.set()

    def destroy(self):
        self.__stop_polling()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.__routine_polling()

        self.__close()

    def __initialize(self):
        self._socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        if self._timeout is None:
            self._socket.setsockopt(zmq.RCVTIMEO, -1)
        else:
            timeout_milliseconds = int(self._timeout.total_seconds() * 1000)
            self._socket.setsockopt(zmq.RCVTIMEO, timeout_milliseconds)

        # Subscribe to all messages: ZMQAsyncPublisher / YMQAsyncPublisher send payloads
        # without a topic prefix, so the subscription filter must be empty to match.
        self._socket.subscribe(b"")
        self._socket.connect(repr(self._address))

    def __routine_polling(self):
        try:
            self.__routine_receive(self._socket.recv(copy=False).bytes)
        except zmq.Again:
            assert self._timeout is not None
            raise TimeoutError(f"Cannot connect to {self._address!r} in {self._timeout.total_seconds()} seconds")

    def __routine_receive(self, payload: bytes):
        result: Optional[BaseMessage] = deserialize(payload)
        if result is None:
            logging.error(f"received unknown message: {payload!r}")
            return None

        self._callback(result)
