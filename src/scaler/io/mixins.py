import abc
from enum import Enum
from typing import Awaitable, Callable, Optional

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import BaseMessage, BinderStatus
from scaler.utility.identifiers import ObjectID
from scaler.utility.mixins import Looper, Reporter


class ConnectorRemoteType(Enum):
    # Connector connects to a binder
    Binder = "binder"

    # Connector connects to another connector
    Connector = "connector"


class NetworkBackend(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def create_internal_address(name: str, same_process: bool) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_binder(
        self, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]
    ) -> "AsyncBinder":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_connector(
        self, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
    ) -> "AsyncConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_publisher(self, identity: bytes) -> "AsyncPublisher":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_connector(
        self, identity: bytes, connector_remote_type: ConnectorRemoteType, address: AddressConfig
    ) -> "SyncConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_async_object_storage_connector(self, identity: bytes) -> "AsyncObjectStorageConnector":
        raise NotImplementedError()

    @abc.abstractmethod
    def create_sync_object_storage_connector(
        self, identity: bytes, address: AddressConfig
    ) -> "SyncObjectStorageConnector":
        raise NotImplementedError()


class AsyncBinder(Looper, Reporter, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def bind(self, address: AddressConfig) -> None:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, to: bytes, message: BaseMessage):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self) -> BinderStatus:
        raise NotImplementedError()


class AsyncConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, address: AddressConfig, remote_type: ConnectorRemoteType) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    async def bind(self, address: AddressConfig) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: BaseMessage):
        raise NotImplementedError()

    @abc.abstractmethod
    async def receive(self) -> Optional[BaseMessage]:
        raise NotImplementedError()


class AsyncPublisher(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def bind(self, address: AddressConfig) -> None:
        raise NotImplementedError()

    @abc.abstractmethod
    def destroy(self):
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def identity(self) -> bytes:
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def address(self) -> Optional[AddressConfig]:
        raise NotImplementedError()

    @abc.abstractmethod
    async def send(self, message: BaseMessage):
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
    def address(self) -> AddressConfig:
        raise NotImplementedError()

    @abc.abstractmethod
    def send(self, message: BaseMessage):
        raise NotImplementedError()

    @abc.abstractmethod
    def receive(self) -> Optional[BaseMessage]:
        raise NotImplementedError()


class AsyncObjectStorageConnector(Looper, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def connect(self, address: AddressConfig):
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
    def address(self) -> Optional[AddressConfig]:
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
    def address(self) -> AddressConfig:
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
