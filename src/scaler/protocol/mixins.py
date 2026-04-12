from typing import TypeVar

from scaler.protocol.capnp import CapnpStruct, Message as CapnpMessage

Message = CapnpMessage
MessageType = TypeVar("MessageType", bound=Message)
