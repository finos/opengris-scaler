import abc
from typing import Awaitable, Callable, Optional

from scaler.protocol.python.mixins import Message
from scaler.protocol.python.status import BinderStatus
from scaler.utility.identifiers import ObjectID
from scaler.utility.mixins import Looper, Reporter


class AsyncBinder(Looper, Reporter, metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def identity(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def register(self, callback: Callable[[bytes, Message], Awaitable[None]]):
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, to: bytes, message: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self) -> BinderStatus:
        raise NotImplementedError()


class AsyncConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self) -> Optional[Message]:
        raise NotImplementedError()


class SyncConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def send(self, message: Message):
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self) -> Optional[Message]:
        raise NotImplementedError()


class AsyncObjectStorageConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, host: str, port: int):
        raise NotImplementedError()

    @abc.abstractmethod
    async def wait_until_connected(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def is_connected(self) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    async def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytes:
        raise NotImplementedError()

    @abc.abstractmethod
    async def delete_object(self, object_id: ObjectID) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        raise NotImplementedError()


class SyncObjectStorageConnector(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def set_object(self, object_id: ObjectID, payload: bytes):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytearray:
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_object(self, object_id: ObjectID) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        raise NotImplementedError()


class SyncSubscriber(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self) -> None:
        raise NotImplementedError()
