import os
import tempfile
from datetime import timedelta
from typing import Awaitable, Callable, Optional

from scaler.config.defaults import SCALER_NETWORK_BACKEND
from scaler.config.types.address import AddressConfig, SocketType
from scaler.config.types.network_backend import NetworkBackendType
from scaler.io import ymq
from scaler.io.mixins import (
    AsyncBinder,
    AsyncConnector,
    AsyncObjectStorageConnector,
    AsyncPublisher,
    ConnectorRemoteType,
    NetworkBackend,
    SyncConnector,
    SyncObjectStorageConnector,
    SyncSubscriber,
)
from scaler.io.ymq_async_binder import YMQAsyncBinder
from scaler.io.ymq_async_connector import YMQAsyncConnector
from scaler.io.ymq_async_object_storage_connector import YMQAsyncObjectStorageConnector
from scaler.io.ymq_async_publisher import YMQAsyncPublisher
from scaler.io.ymq_sync_connector import YMQSyncConnector
from scaler.io.ymq_sync_object_storage_connector import YMQSyncObjectStorageConnector
from scaler.io.ymq_sync_subscriber import YMQSyncSubscriber
from scaler.protocol.capnp import BaseMessage


class ZMQNetworkBackend(NetworkBackend):
    def __init__(self, io_threads: int):
        try:
            import zmq
            import zmq.asyncio
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError('execute "pip install opengris-scaler[zmq]" to use ZMQ network backend') from exc

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
        from scaler.io.zmq_async_binder import ZMQAsyncBinder

        return ZMQAsyncBinder(context=self._async_context, identity=identity, callback=callback)

    def create_async_connector(
        self, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
    ) -> AsyncConnector:
        from scaler.io.zmq_async_connector import ZMQAsyncConnector

        return ZMQAsyncConnector(context=self._async_context, identity=identity, callback=callback)

    def create_async_publisher(self, identity: bytes) -> AsyncPublisher:
        from scaler.io.zmq_async_publisher import ZMQAsyncPublisher

        return ZMQAsyncPublisher(context=self._async_context, identity=identity)

    def create_sync_connector(
        self, identity: bytes, connector_remote_type: ConnectorRemoteType, address: AddressConfig
    ) -> SyncConnector:
        from scaler.io.zmq_sync_connector import ZMQSyncConnector

        return ZMQSyncConnector(
            context=self._context, identity=identity, connector_remote_type=connector_remote_type, address=address
        )

    def create_async_object_storage_connector(self, identity: bytes) -> AsyncObjectStorageConnector:
        assert self._context is not None
        assert self._object_storage_context is not None
        return YMQAsyncObjectStorageConnector(context=self._object_storage_context, identity=identity)

    def create_sync_object_storage_connector(
        self, identity: bytes, address: AddressConfig
    ) -> SyncObjectStorageConnector:
        assert self._context is not None
        assert self._object_storage_context is not None
        return YMQSyncObjectStorageConnector(context=self._object_storage_context, identity=identity, address=address)

    def create_sync_subscriber(
        self,
        identity: bytes,
        address: AddressConfig,
        callback: Callable[[BaseMessage], None],
        timeout: Optional[timedelta],
    ) -> SyncSubscriber:
        from scaler.io.zmq_sync_subscriber import ZMQSyncSubscriber

        return ZMQSyncSubscriber(
            context=self._context, identity=identity, address=address, callback=callback, timeout=timeout
        )


class YMQNetworkBackend(NetworkBackend):
    def __init__(self, num_threads: int):
        self._context: Optional[ymq.IOContext] = ymq.IOContext(num_threads=num_threads)

        self._destroyed = False

    def __del__(self):
        self.destroy()

    def destroy(self):
        self._destroyed = True

        self._context = None

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
        assert self._context is not None
        return YMQAsyncPublisher(context=self._context, identity=identity)

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

    def create_sync_subscriber(
        self,
        identity: bytes,
        address: AddressConfig,
        callback: Callable[[BaseMessage], None],
        timeout: Optional[timedelta],
    ) -> SyncSubscriber:
        assert self._context is not None
        return YMQSyncSubscriber(
            context=self._context, identity=identity, address=address, callback=callback, timeout=timeout
        )


def get_scaler_network_backend_type_from_env() -> NetworkBackendType:
    backend_str = os.environ.get("SCALER_NETWORK_BACKEND")
    if backend_str is None:
        return SCALER_NETWORK_BACKEND

    return NetworkBackendType[backend_str]


def get_network_backend_from_env(io_threads: int = 1) -> NetworkBackend:
    backend = get_scaler_network_backend_type_from_env()

    if backend == NetworkBackendType.zmq:
        return ZMQNetworkBackend(io_threads=io_threads)
    elif backend == NetworkBackendType.ymq:
        return YMQNetworkBackend(num_threads=io_threads)

    raise ValueError(
        f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackendType]}"
    )
