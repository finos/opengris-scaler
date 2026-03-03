from typing import Optional

from scaler.io.uv_ymq import _uv_ymq
from scaler.io.ymq.ymq import call_async, call_sync


class BinderSocket:
    __doc__ = _uv_ymq.BinderSocket.__doc__

    _base: _uv_ymq.BinderSocket

    def __init__(self, context: _uv_ymq.IOContext, identity: str) -> None:
        self._base = _uv_ymq.BinderSocket(context, identity)

    @property
    def identity(self) -> str:
        return self._base.identity

    async def bind_to(self, address: str) -> _uv_ymq.Address:
        return await call_async(self._base.bind_to, address)

    def bind_to_sync(self, address: str, /, timeout: Optional[float] = None) -> _uv_ymq.Address:
        return call_sync(self._base.bind_to, address, timeout=timeout)

    async def send_message(self, remote_identity: str, message_payload: _uv_ymq.Bytes) -> None:
        await call_async(self._base.send_message, remote_identity, message_payload)

    def send_message_sync(
        self, remote_identity: str, message_payload: _uv_ymq.Bytes, /, timeout: Optional[float] = None
    ) -> None:
        call_sync(self._base.send_message, remote_identity, message_payload, timeout=timeout)

    async def recv_message(self) -> _uv_ymq.Message:
        return await call_async(self._base.recv_message)

    def recv_message_sync(self, /, timeout: Optional[float] = None) -> _uv_ymq.Message:
        return call_sync(self._base.recv_message, timeout=timeout)

    def close_connection(self, remote_identity: str) -> None:
        self._base.close_connection(remote_identity)


class ConnectorSocket:
    __doc__ = _uv_ymq.ConnectorSocket.__doc__

    _base: _uv_ymq.ConnectorSocket

    def __init__(
        self,
        context: _uv_ymq.IOContext,
        identity: str,
        address: str,
        max_retry_times: int = _uv_ymq.DEFAULT_MAX_RETRY_TIMES,
        init_retry_delay: int = _uv_ymq.DEFAULT_INIT_RETRY_DELAY,
    ) -> None:
        def create(callback, *args, **kwargs):
            self._base = _uv_ymq.ConnectorSocket(callback, *args, **kwargs)

        call_sync(create, context, identity, address, max_retry_times, init_retry_delay)

    @property
    def identity(self) -> str:
        return self._base.identity

    async def send_message(self, message_payload: _uv_ymq.Bytes) -> None:
        await call_async(self._base.send_message, message_payload)

    def send_message_sync(self, message_payload: _uv_ymq.Bytes, /, timeout: Optional[float] = None) -> None:
        call_sync(self._base.send_message, message_payload, timeout=timeout)

    async def recv_message(self) -> _uv_ymq.Message:
        return await call_async(self._base.recv_message)

    def recv_message_sync(self, /, timeout: Optional[float] = None) -> _uv_ymq.Message:
        return call_sync(self._base.recv_message, timeout=timeout)
