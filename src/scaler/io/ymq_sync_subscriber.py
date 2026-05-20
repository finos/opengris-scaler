import logging
import threading
from datetime import timedelta
from typing import Callable, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import SyncSubscriber
from scaler.io.utility import deserialize
from scaler.io.ymq import ConnectorSocket, IOContext
from scaler.protocol.capnp import BaseMessage


class YMQSyncSubscriber(SyncSubscriber):
    def __init__(
        self,
        context: IOContext,
        identity: bytes,
        address: AddressConfig,
        callback: Callable[[BaseMessage], None],
        timeout: Optional[timedelta] = None,
    ):
        super().__init__()

        self._stop_event = threading.Event()

        self._ymq_context = context
        self._identity = identity
        self._address = address
        self._callback = callback
        self._timeout = timeout

        self._socket = ConnectorSocket.connect(self._ymq_context, self._identity.decode(), repr(self._address))

    def __close(self):
        self._socket.shutdown()

    def __stop_polling(self):
        self._stop_event.set()

    def destroy(self):
        self.__stop_polling()

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.__routine_polling()

        self.__close()

    def __routine_polling(self):
        timeout_seconds = self._timeout.total_seconds() if self._timeout is not None else None

        try:
            message = self._socket.recv_message_sync(timeout=timeout_seconds)
            payload_data = message.payload.data
            if payload_data is None:
                return

            self.__routine_receive(payload_data)
        except TimeoutError:
            raise TimeoutError(f"Cannot connect to {self._address!r} in {timeout_seconds} seconds")

    def __routine_receive(self, payload: bytes):
        result: Optional[BaseMessage] = deserialize(payload)
        if result is None:
            logging.error(f"received unknown message: {payload!r}")
            return

        self._callback(result)
