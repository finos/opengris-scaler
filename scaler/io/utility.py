import logging
from typing import Optional

from scaler.io.config import MESSAGE_SIZE_LIMIT
from scaler.protocol.capnp._python import _message  # noqa
from scaler.protocol.python.message import PROTOCOL
from scaler.protocol.python.mixins import Message


def deserialize(data: bytes) -> Optional[Message]:
    with _message.Message.from_bytes(data, traversal_limit_in_words=MESSAGE_SIZE_LIMIT) as payload:
        if not hasattr(payload, payload.which()):
            logging.error(f"unknown message type: {payload.which()}")
            return None

        message = getattr(payload, payload.which())
        return PROTOCOL[payload.which()](message)


def serialize(message: Message) -> bytes:
    payload = _message.Message(**{PROTOCOL.inverse[type(message)]: message.get_message()})
    return payload.to_bytes()
