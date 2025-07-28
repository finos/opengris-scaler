import logging
import threading
from typing import Callable, Optional

import sys
if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = object

from scaler.io.ymq.ymq import IOContext, IOSocket, IOSocketType

from scaler.io.utility import deserialize
from scaler.protocol.python.mixins import Message
from scaler.utility.identifiers import ClientID
from scaler.utility.ymq_config import YMQConfig


class SyncSubscriber(threading.Thread):
    def __init__(
        self,
        address: YMQConfig,
        callback: Callable[[Message], None],
        topic: str,
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

        self._context: Optional[IOContext] = None
        self._socket: Optional[IOSocket] = None

    def __close(self):
        pass

    def __stop_polling(self):
        self._stop_event.set()

    def disconnect(self):
        self.__stop_polling()

    def run(self) -> None:
        self.__initialize()

        while not self._stop_event.is_set():
            self.__routine_polling()

        if self._exit_callback is not None:
            self._exit_callback()

        self.__close()

    def __initialize(self):
        self._context = IOContext(num_threads=1)
        self._socket = self._context.createIOSocket_sync(
            identity=repr(ClientID.generate_client_id()),
            socket_type=IOSocketType.Unicast
        )

        # todo: implement topic subscription in ymq
        # self._socket.subscribe(self._topic)
        self._socket.connect_sync(self._address.to_address())

    def __routine_polling(self):
        if self._socket is None:
            raise RuntimeError("Socket not initialized")

        self.__routine_receive(self._socket.recv_sync().payload)

    def __routine_receive(self, payload: Buffer):
        result: Optional[Message] = deserialize(payload)
        if result is None:
            logging.error(f"received unknown message: {payload!r}")
            return None

        self._callback(result)
