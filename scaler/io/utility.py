import logging
from typing import List, Optional

from scaler.io.config import CAPNP_DATA_SIZE_LIMIT, CAPNP_MESSAGE_SIZE_LIMIT
from scaler.protocol.capnp._python import _message  # noqa
from scaler.protocol.python.message import PROTOCOL
from scaler.protocol.python.mixins import Message


def deserialize(data: bytes) -> Optional[Message]:
    with _message.Message.from_bytes(data, traversal_limit_in_words=CAPNP_MESSAGE_SIZE_LIMIT) as payload:
        if not hasattr(payload, payload.which()):
            logging.error(f"unknown message type: {payload.which()}")
            return None

        message = getattr(payload, payload.which())
        return PROTOCOL[payload.which()](message)


def serialize(message: Message) -> bytes:
    payload = _message.Message(**{PROTOCOL.inverse[type(message)]: message.get_message()})
    return payload.to_bytes()


def chunk_to_list_of_bytes(data: bytes) -> List[bytes]:
    # TODO: change to list of memoryview when capnp can support memoryview
    return [data[i : i + CAPNP_DATA_SIZE_LIMIT] for i in range(0, len(data), CAPNP_DATA_SIZE_LIMIT)]


def concat_list_of_bytes(data: List[bytes]) -> bytes:
    return bytearray().join(data)
