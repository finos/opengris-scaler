import logging
import os
from typing import List, Optional

import zmq.asyncio

from scaler.config.defaults import CAPNP_DATA_SIZE_LIMIT, CAPNP_MESSAGE_SIZE_LIMIT, SCALER_NETWORK_BACKEND
from scaler.config.types.network_backend import NetworkBackend
from scaler.io.async_object_storage_connector import PyAsyncObjectStorageConnector
from scaler.io.mixins import AsyncBinder, AsyncConnector, AsyncObjectStorageConnector, SyncObjectStorageConnector
from scaler.io.sync_object_storage_connector import PySyncObjectStorageConnector
from scaler.protocol.capnp._python import _message  # noqa
from scaler.protocol.python.message import PROTOCOL
from scaler.protocol.python.mixins import Message

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer


def get_scaler_network_backend_from_env():
    backend_str = os.environ.get("SCALER_NETWORK_BACKEND")  # Default to tcp_zmq
    if backend_str is None:
        return SCALER_NETWORK_BACKEND
    return NetworkBackend[backend_str]


def create_async_binder(ctx: zmq.asyncio.Context, *args, **kwargs) -> AsyncBinder:
    connector_type = get_scaler_network_backend_from_env()
    if connector_type == NetworkBackend.ymq:
        from scaler.io.ymq_async_binder import YMQAsyncBinder

        return YMQAsyncBinder(*args, **kwargs)
    elif connector_type == NetworkBackend.tcp_zmq:
        from scaler.io.async_binder import ZMQAsyncBinder

        return ZMQAsyncBinder(context=ctx, *args, **kwargs)  # type: ignore[misc]
    else:
        raise ValueError(
            f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackend]}"
        )


def create_async_connector(ctx: zmq.asyncio.Context, *args, **kwargs) -> AsyncConnector:
    connector_type = get_scaler_network_backend_from_env()
    if connector_type == NetworkBackend.ymq:
        from scaler.io.ymq import ymq
        from scaler.io.ymq_async_connector import YMQAsyncConnector

        kwargs["socket_type"] = ymq.IOSocketType.Connector
        return YMQAsyncConnector(*args, **kwargs)
    elif connector_type == NetworkBackend.tcp_zmq:
        from scaler.io.async_connector import ZMQAsyncConnector

        return ZMQAsyncConnector(context=ctx, *args, **kwargs)  # type: ignore[misc]
    else:
        raise ValueError(
            f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackend]}"
        )


def create_async_object_storage_connector(*args, **kwargs) -> AsyncObjectStorageConnector:
    connector_type = get_scaler_network_backend_from_env()
    if connector_type == NetworkBackend.ymq:
        from scaler.io.ymq_async_object_storage_connector import PyYMQAsyncObjectStorageConnector

        return PyYMQAsyncObjectStorageConnector(*args, **kwargs)

    elif connector_type == NetworkBackend.tcp_zmq:
        return PyAsyncObjectStorageConnector(*args, **kwargs)

    else:
        raise ValueError(
            f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackend]}"
        )


def create_sync_object_storage_connector(*args, **kwargs) -> SyncObjectStorageConnector:
    connector_type = get_scaler_network_backend_from_env()
    if connector_type == NetworkBackend.ymq:
        from scaler.io.ymq_sync_object_storage_connector import PyYMQSyncObjectStorageConnector

        return PyYMQSyncObjectStorageConnector(*args, **kwargs)

    elif connector_type == NetworkBackend.tcp_zmq:
        return PySyncObjectStorageConnector(*args, **kwargs)
    else:
        raise ValueError(
            f"Invalid SCALER_NETWORK_BACKEND value." f"Expected one of: {[e.name for e in NetworkBackend]}"
        )


def deserialize(data: Buffer) -> Optional[Message]:
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
