import logging
import threading
from typing import Callable, List, Optional

import zmq

from scaler.protocol.python.message import PROTOCOL, MessageType, MessageVariant
from scaler.utility.zmq_config import ZMQConfig


class SyncSubscriber(threading.Thread):
    def __init__(
        self,
        address: ZMQConfig,
        callback: Callable[[MessageVariant], None],
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

        self._context: Optional[zmq.Context] = None
        self._socket: Optional[zmq.Socket] = None

    def __close(self):
        self._socket.close()

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
        self._context = zmq.Context.instance()
        self._socket: zmq.Socket = self._context.socket(zmq.SUB)
        self._socket.setsockopt(zmq.RCVHWM, 0)

        if self._timeout_seconds == -1:
            self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_seconds)
        else:
            self._socket.setsockopt(zmq.RCVTIMEO, self._timeout_seconds * 1000)

        self._socket.subscribe(self._topic)
        self._socket.connect(self._address.to_address())
        self._socket.connect(self._address.to_address())

    def __routine_polling(self):
        try:
            frames = self._socket.recv_multipart()
            self.__routine_receive(frames)
        except zmq.Again:
            raise TimeoutError(f"Cannot connect to {self._address.to_address()} in {self._timeout_seconds} seconds")

    def __routine_receive(self, frames: List[bytes]):
        logging.info(frames)
        if len(frames) < 2:
            logging.error(f"received unexpected frames {frames}")
            return

        if frames[0] not in {member.value for member in MessageType}:
            logging.error(f"received unexpected message type: {frames[0]}")
            return

        message_type_bytes, *payload = frames
        message_type = MessageType(message_type_bytes)
        message = PROTOCOL[message_type].deserialize(payload)

        self._callback(message)
