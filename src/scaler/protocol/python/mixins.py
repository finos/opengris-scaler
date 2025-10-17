import abc
from typing import TypeVar


class Message(metaclass=abc.ABCMeta):
    def __init__(self, msg):
        self._msg = msg

    def get_message(self):
        return self._msg


MessageType = TypeVar("MessageType", bound=Message)
