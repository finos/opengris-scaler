import os
import tempfile
from typing import Awaitable, Callable, Optional

import zmq
import zmq.asyncio

from scaler.config.defaults import SCALER_NETWORK_BACKEND
from scaler.config.types.address import AddressConfig, SocketType
from scaler.config.types.network_backend import NetworkBackendType
from scaler.io import ymq
from scaler.io.async_binder import ZMQAsyncBinder
from scaler.io.async_connector import ZMQAsyncConnector
from scaler.io.async_publisher import ZMQAsyncPublisher
from scaler.io.mixins import (
    AsyncBinder,
    AsyncConnector,
    AsyncObjectStorageConnector,
    AsyncPublisher,
    ConnectorRemoteType,
    NetworkBackend,
    SyncConnector,
    SyncObjectStorageConnector,
)
from scaler.io.sync_connector import ZMQSyncConnector
from scaler.io.ymq_async_binder import YMQAsyncBinder
from scaler.io.ymq_async_connector import YMQAsyncConnector
from scaler.io.ymq_async_object_storage_connector import YMQAsyncObjectStorageConnector
from scaler.io.ymq_sync_connector import YMQSyncConnector
from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector
from scaler.protocol.capnp import BaseMessage


class ZMQNetworkBackend(NetworkBackend):
    def __init__(self, io_threads: int):
        self._context = zmq.Context(io_threads=io_threads)
        self._async_context = zmq.asyncio.Context.shadow(self._context)

        self._object_storage_context: Optional[ymq.IOContext] = ymq.IOContext(num_threads=io_threads)

        self._destroyed = False

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._destroyed:
            return

        self._destroyed = True

        self._context.destroy(linger=0)
        self._object_storage_context = None

    @staticmethod
    def create_internal_address(name: str, same_process: bool) -> AddressConfig:
        if same_process:
            return AddressConfig(SocketType.inproc, host=name)
        else:
            ipc_path = os.path.join(tempfile.gettempdir(), name)
            return AddressConfig(SocketType.ipc, host=ipc_path)

    def create_async_binder(
        self, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]
    ) -> AsyncBinder:
        return ZMQAsyncBinder(context=self._async_context, identity=identity, callback=callback)

    def create_async_connector(
        self, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
    ) -> AsyncConnector:
        return ZMQAsyncConnector(context=self._async_context, identity=identity, callback=callback)

    def create_async_publisher(self, identity: bytes) -> AsyncPublisher:
        return ZMQAsyncPublisher(context=self._async_context, identity=identity)

    def create_sync_connector(
        self, identity: bytes, connector_remote_type: ConnectorRemoteType, address: AddressConfig
    ) -> SyncConnector:
        return ZMQSyncConnector(
            context=self._context, identity=identity, connector_remote_type=connector_remote_type, address=address
        )

    def create_async_object_storage_connector(self, identity: bytes) -> AsyncObjectStorageConnector:
        assert self._context is not None
        return YMQAsyncObjectStorageConnector(context=self._object_storage_context, identity=identity)

    def create_sync_object_storage_connector(
        self, identity: bytes, address: AddressConfig
    ) -> SyncObjectStorageConnector:
        assert self._context is not None
        return YMQSyncObjectStorageConnector(context=self._object_storage_context, identity=identity, address=address)


class YMQNetworkBackend(NetworkBackend):
    def __init__(self, num_threads: int):
        self._context: Optional[ymq.IOContext] = ymq.IOContext(num_threads=num_threads)

        self._publisher_context = zmq.asyncio.Context(io_threads=num_threads)

        self._destroyed = False

    def __del__(self):
        self.destroy()

    def destroy(self):
        self._destroyed = True

        self._context = None
        self._publisher_context.destroy(linger=0)

    @staticmethod
    def create_internal_address(name: str, same_process: bool) -> AddressConfig:
        ipc_path = os.path.join(tempfile.gettempdir(), name)
        return AddressConfig(SocketType.ipc, host=ipc_path)

    def create_async_binder(
        self, identity: bytes, callback: Callable[[bytes, BaseMessage], Awaitable[None]]
    ) -> AsyncBinder:
        assert self._context is not None
        return YMQAsyncBinder(context=self._context, identity=identity, callback=callback)

    def create_async_connector(
        self, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
    ) -> AsyncConnector:
        assert self._context is not None
        return YMQAsyncConnector(context=self._context, identity=identity, callback=callback)

    def create_async_publisher(self, identity: bytes) -> AsyncPublisher:
        return ZMQAsyncPublisher(context=self._publisher_context, identity=identity)

    def create_sync_connector(
        self, identity: bytes, connector_remote_type: ConnectorRemoteType, address: AddressConfig
    ) -> SyncConnector:
        assert self._context is not None
        return YMQSyncConnector(context=self._context, identity=identity, address=address)

    def create_async_object_storage_connector(self, identity: bytes) -> AsyncObjectStorageConnector:
        assert self._context is not None
        return YMQAsyncObjectStorageConnector(context=self._context, identity=identity)

    def create_sync_object_storage_connector(
        self, identity: bytes, address: AddressConfig
    ) -> SyncObjectStorageConnector:
        assert self._context is not None
        return YMQSyncObjectStorageConnector(context=self._context, identity=identity, address=address)


def get_scaler_network_backend_type_from_env() -> NetworkBackendType:
    backend_str = os.environ.get("SCALER_NETWORK_BACKEND")  # Default to tcp_zmq
    if backend_str is None:
        return SCALER_NETWORK_BACKEND

    return NetworkBackendType[backend_str]


def get_network_backend_from_env(io_threads: int = 1) -> NetworkBackend:
    backend = get_scaler_network_backend_type_from_env()

    if backend == NetworkBackendType.tcp_zmq:
        return ZMQNetworkBackend(io_threads=io_threads)
    elif backend == NetworkBackendType.ymq:
        return YMQNetworkBackend(num_threads=io_threads)

    raise ValueError(
        f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackendType]}"
    )
