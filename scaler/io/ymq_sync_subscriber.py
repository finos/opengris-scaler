import logging
import threading
import random
from typing import Callable, Optional

from scaler.io.ymq import ymq

from scaler.io.mixins import SyncSubscriber
from scaler.io.utility import deserialize
from scaler.protocol.python.mixins import Message
from scaler.utility.zmq_config import ZMQConfig, ZMQType


class YMQSyncSubscriber(SyncSubscriber, threading.Thread):
    def __init__(
        self,
        address: ZMQConfig,
        callback: Callable[[Message], None],
        topic: bytes,
        exit_callback: Optional[Callable[[], None]] = None,
        stop_event: threading.Event = threading.Event(),
        daemonic: bool = False,
        timeout_seconds: int = -1,
    ):
        threading.Thread.__init__(self)

        self._stop_event = stop_event
        self._address = address
        self._callback = callback
        self._exit_callback = exit_callback
        self._topic = topic
        self.daemon = bool(daemonic)
        self._timeout_seconds = timeout_seconds

        self._context: Optional[ymq.IOContext] = None
        self._socket: Optional[ymq.IOSocket] = None

    def __close(self):
        self._context = None
        self._socket = None

    def __stop_polling(self):
        self._stop_event.set()

    def destroy(self):
        self.__stop_polling()

    def run(self) -> None:
        self.__initialize()

        while not self._stop_event.is_set():
            self.__routine_polling()

        if self._exit_callback is not None:
            self._exit_callback()

        self.__close()

    def __initialize(self):
        self._context = ymq.IOContext()
        self._socket = self._context.createIOSocket_sync(f"{self._topic.decode()}_subscriber_{random.randint(10_000, 20_000)}", ymq.IOSocketType.Unicast)

        if self._address.type != ZMQType.tcp:
            raise ValueError(f"YMQ only supports tcp transport, got {self._address.type}")

        self._socket.connect_sync(self._address.to_address())

    def __routine_polling(self):
        try:
            # TODO: zero-copy
            self.__routine_receive(self._socket.recv_sync(timeout_secs=1).payload.data)
        except ymq.YMQTimeoutException:
            pass

    def __routine_receive(self, payload: bytes):
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"received unknown message: {payload!r}")
            return None

        self._callback(result)
