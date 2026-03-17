from typing import Optional

from scaler.io.ymq import _ymq
from scaler.io.ymq.utils import call_async, call_sync


class BinderSocket:
    __doc__ = _ymq.BinderSocket.__doc__

    _base: _ymq.BinderSocket

    def __init__(self, context: _ymq.IOContext, identity: str) -> None:
        self._base = _ymq.BinderSocket(context, identity)

    @property
    def identity(self) -> str:
        return self._base.identity

    async def bind_to(self, address: str) -> _ymq.Address:
        return await call_async(self._base.bind_to, address)

    def bind_to_sync(self, address: str, /, timeout: Optional[float] = None) -> _ymq.Address:
        return call_sync(self._base.bind_to, address, timeout=timeout)

    async def send_message(self, remote_identity: str, message_payload: _ymq.Bytes) -> None:
        await call_async(self._base.send_message, remote_identity, message_payload)

    def send_message_sync(
        self, remote_identity: str, message_payload: _ymq.Bytes, /, timeout: Optional[float] = None
    ) -> None:
        call_sync(self._base.send_message, remote_identity, message_payload, timeout=timeout)

    async def recv_message(self) -> _ymq.Message:
        return await call_async(self._base.recv_message)

    def recv_message_sync(self, /, timeout: Optional[float] = None) -> _ymq.Message:
        return call_sync(self._base.recv_message, timeout=timeout)

    def close_connection(self, remote_identity: str) -> None:
        self._base.close_connection(remote_identity)


class ConnectorSocket:
    __doc__ = _ymq.ConnectorSocket.__doc__

    _base: _ymq.ConnectorSocket

    def __init__(self, base: _ymq.ConnectorSocket) -> None:
        self._base = base

    @staticmethod
    def connect(
        context: _ymq.IOContext,
        identity: str,
        address: str,
        max_retry_times: int = _ymq.DEFAULT_MAX_RETRY_TIMES,
        init_retry_delay: int = _ymq.DEFAULT_INIT_RETRY_DELAY,
    ) -> "ConnectorSocket":
        base_socket: Optional[_ymq.ConnectorSocket] = None

        def create(callback, *args, **kwargs):
            nonlocal base_socket
            base_socket = _ymq.ConnectorSocket.connect(callback, *args, **kwargs)

        call_sync(create, context, identity, address, max_retry_times, init_retry_delay)
        assert base_socket is not None

        return ConnectorSocket(base_socket)

    @staticmethod
    def bind(context: _ymq.IOContext, identity: str, address: str) -> "ConnectorSocket":
        base_socket: Optional[_ymq.ConnectorSocket] = None

        def create(callback, *args, **kwargs):
            nonlocal base_socket
            base_socket = _ymq.ConnectorSocket.bind(callback, *args, **kwargs)

        call_sync(create, context, identity, address)
        assert base_socket is not None

        return ConnectorSocket(base_socket)

    @property
    def identity(self) -> str:
        return self._base.identity

    async def send_message(self, message_payload: _ymq.Bytes) -> None:
        await call_async(self._base.send_message, message_payload)

    def send_message_sync(self, message_payload: _ymq.Bytes, /, timeout: Optional[float] = None) -> None:
        call_sync(self._base.send_message, message_payload, timeout=timeout)

    async def recv_message(self) -> _ymq.Message:
        return await call_async(self._base.recv_message)

    def recv_message_sync(self, /, timeout: Optional[float] = None) -> _ymq.Message:
        return call_sync(self._base.recv_message, timeout=timeout)
