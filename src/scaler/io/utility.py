import logging
import os
import uuid
from typing import List, Optional

from scaler.config.defaults import CAPNP_DATA_SIZE_LIMIT, CAPNP_MESSAGE_SIZE_LIMIT
from scaler.protocol.capnp import BaseMessage, Message
from scaler.protocol.helpers import PROTOCOL

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer


def generate_identity_from_name(name: str) -> bytes:
    return f"{os.getpid()}|{name}|{uuid.uuid4()}".encode()


def deserialize(data: Buffer) -> Optional[BaseMessage]:
    payload = Message.from_bytes(bytes(data), traversal_limit_in_words=CAPNP_MESSAGE_SIZE_LIMIT)
    if not hasattr(payload, payload.which()):
        logging.error(f"unknown message type: {payload.which()}")
        return None

    return getattr(payload, payload.which())


def serialize(message: BaseMessage) -> bytes:
    payload = Message(**{PROTOCOL.inverse[type(message)]: message})
    return payload.to_bytes()


def chunk_to_list_of_bytes(data: bytes) -> List[bytes]:
    # TODO: change to list of memoryview when capnp can support memoryview
    return [data[i : i + CAPNP_DATA_SIZE_LIMIT] for i in range(0, len(data), CAPNP_DATA_SIZE_LIMIT)]


def concat_list_of_bytes(data: List[bytes]) -> bytes:
    return bytearray().join(data)
