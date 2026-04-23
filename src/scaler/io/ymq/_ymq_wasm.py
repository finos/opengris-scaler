"""Pure-Python ymq transport for Pyodide / Emscripten browser environments.

This module mirrors the public surface of the ``scaler.io.ymq._ymq`` C extension
on platforms where the C extension cannot be loaded (i.e. ``sys.platform ==
"emscripten"``). It is selected by ``scaler.io.ymq.__init__`` at import time and
must NOT be used directly.

It implements the YMQ wire protocol over a single ``WebSocket`` opened from the
browser (via ``js.WebSocket``). The wire protocol is byte-for-byte compatible
with the native C++ implementation:

    1. After the WebSocket Upgrade handshake (handled by the browser), each
       endpoint sends a 4-byte magic string ``YMQ\\x01``.
    2. Each endpoint then sends its identity as a length-prefixed message
       (8-byte little-endian length followed by the identity bytes).
    3. Subsequent application messages use the same length-prefixed framing.

Only ``ConnectorSocket.connect`` is implemented; ``BinderSocket`` and
``ConnectorSocket.bind`` are not supported in the browser (the browser cannot
listen for inbound TCP/WebSocket connections).
"""

from __future__ import annotations

import struct
import sys
from collections import deque
from enum import IntEnum
from typing import Any, Callable, Deque, List, Optional, Union

# Match the values exposed by the native module and documented in _ymq.pyi.
DEFAULT_MAX_RETRY_TIMES: int = 8
DEFAULT_INIT_RETRY_DELAY: int = 100  # milliseconds

# YMQ wire-protocol constants (mirror src/cpp/scaler/ymq/configuration.h).
_MAGIC_STRING: bytes = b"YMQ\x01"
_HEADER_FORMAT: str = "<Q"  # uint64_t little-endian, matches C++ ``Header``
_HEADER_SIZE: int = struct.calcsize(_HEADER_FORMAT)


# ---------------------------------------------------------------------------
# Errors


class ErrorCode(IntEnum):
    Uninit = 0
    InvalidPortFormat = 1
    InvalidAddressFormat = 2
    RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery = 3
    ConnectorSocketClosedByRemoteEnd = 4
    SocketStopRequested = 5
    SysCallError = 6

    def explanation(self) -> str:
        return {
            ErrorCode.Uninit: "Uninitialised error code",
            ErrorCode.InvalidPortFormat: "Invalid port format",
            ErrorCode.InvalidAddressFormat: "Invalid address format",
            ErrorCode.RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery: (
                "Remote end disconnected on a socket without guaranteed delivery"
            ),
            ErrorCode.ConnectorSocketClosedByRemoteEnd: "Connector socket closed by remote end",
            ErrorCode.SocketStopRequested: "Socket stop requested",
            ErrorCode.SysCallError: "System call error",
        }[self]


class YMQException(Exception):
    code: ErrorCode
    message: str

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(f"[{code.name}] {message}")
        self.code = code
        self.message = message

    def __repr__(self) -> str:
        return f"{type(self).__name__}(code={self.code!r}, message={self.message!r})"


class InvalidPortFormatError(YMQException):
    pass


class InvalidAddressFormatError(YMQException):
    pass


class RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError(YMQException):
    pass


class ConnectorSocketClosedByRemoteEndError(YMQException):
    pass


class SocketStopRequestedError(YMQException):
    pass


class SysCallError(YMQException):
    pass


_ERROR_CODE_TO_CLASS = {
    ErrorCode.InvalidPortFormat: InvalidPortFormatError,
    ErrorCode.InvalidAddressFormat: InvalidAddressFormatError,
    ErrorCode.RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery: (
        RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError
    ),
    ErrorCode.ConnectorSocketClosedByRemoteEnd: ConnectorSocketClosedByRemoteEndError,
    ErrorCode.SocketStopRequested: SocketStopRequestedError,
    ErrorCode.SysCallError: SysCallError,
}


def _make_exception(code: ErrorCode, message: str) -> YMQException:
    return _ERROR_CODE_TO_CLASS.get(code, YMQException)(code, message)


# ---------------------------------------------------------------------------
# Bytes / Message / Address


class Bytes:
    """Wraps a ``bytes`` buffer. Mirrors the C extension's ``Bytes`` type."""

    __slots__ = ("_data",)

    def __init__(self, data: Optional[Union["Bytes", bytes, bytearray, memoryview]] = None) -> None:
        if data is None:
            self._data: Optional[bytes] = None
        elif isinstance(data, Bytes):
            self._data = data._data
        elif isinstance(data, (bytes, bytearray, memoryview)):
            self._data = bytes(data)
        else:
            # Buffer protocol fallback
            self._data = bytes(memoryview(data))

    @property
    def data(self) -> Optional[bytes]:
        return self._data

    @property
    def len(self) -> int:
        return 0 if self._data is None else len(self._data)

    def __len__(self) -> int:
        return self.len

    def __bytes__(self) -> bytes:
        return b"" if self._data is None else self._data

    def __repr__(self) -> str:
        if self._data is None:
            return "Bytes(None)"
        return f"Bytes(len={len(self._data)})"


class Message:
    __slots__ = ("address", "payload")

    def __init__(
        self,
        address: Optional[Union[Bytes, bytes, bytearray, memoryview]],
        payload: Union[Bytes, bytes, bytearray, memoryview],
    ) -> None:
        self.address: Optional[Bytes] = None if address is None else Bytes(address)
        self.payload: Bytes = payload if isinstance(payload, Bytes) else Bytes(payload)

    def __repr__(self) -> str:
        return f"Message(address={self.address!r}, payload={self.payload!r})"


class AddressType(IntEnum):
    """Address type enum. Mirrors the values exposed by the native pymod."""

    IPC = 0
    TCP = 1
    # WebSocket is not exposed by the native pymod today but we expose it here
    # since browser clients only ever use ws:// addresses. Downstream code does
    # not currently inspect ``Address.type``.
    WebSocket = 2


class Address:
    """Parses a YMQ address string. Only ``ws://`` and ``wss://`` are useful in the browser."""

    __slots__ = ("_raw", "type")

    def __init__(self, address: str) -> None:
        self._raw = address
        self.type = self._classify(address)

    @staticmethod
    def _classify(address: str) -> AddressType:
        if address.startswith("ws://") or address.startswith("wss://"):
            return AddressType.WebSocket
        if address.startswith("tcp://"):
            return AddressType.TCP
        if address.startswith("ipc://"):
            return AddressType.IPC
        raise _make_exception(ErrorCode.InvalidAddressFormat, f"Unknown address scheme: {address!r}")

    def __repr__(self) -> str:
        return self._raw


# ---------------------------------------------------------------------------
# IOContext (no-op stub; threading does not exist in the browser).


class IOContext:
    __slots__ = ("num_threads",)

    def __init__(self, num_threads: int = 1) -> None:
        self.num_threads = num_threads

    def __repr__(self) -> str:
        return f"IOContext(num_threads={self.num_threads})"


# ---------------------------------------------------------------------------
# ConnectorSocket


# Type aliases for callbacks. We use ``Any`` because mypy can't see across
# Pyodide's JS bridge. ``ConnectCallback`` receives ``None`` on success or an
# Exception on failure; ``SendCallback`` is the same. ``RecvCallback`` receives
# a Message on success or an Exception on failure.
ConnectCallback = Callable[[Optional[Exception]], None]
SendCallback = Callable[[Optional[Exception]], None]
RecvCallback = Callable[[Union[Message, Exception]], None]


def _normalize_ws_address(address: str) -> str:
    """Convert YMQ's ws://host:port[/path] to a URL the browser WebSocket accepts."""
    if address.startswith("ws://") or address.startswith("wss://"):
        return address
    raise _make_exception(
        ErrorCode.InvalidAddressFormat,
        f"Browser ConnectorSocket only supports ws:// and wss:// addresses, got {address!r}",
    )


class ConnectorSocket:
    """A connector socket implementing YMQ over a single browser WebSocket.

    Only the connect side is implemented. ``bind`` is not supported in the
    browser since pages cannot accept inbound connections.
    """

    def __init__(self, identity: str, address: str) -> None:
        self.identity: str = identity
        self._address: str = address
        self._ws: Any = None
        self._open: bool = False
        self._closed: bool = False
        self._close_error: Optional[Exception] = None

        # Reassembly of the raw byte stream coming out of the WebSocket.
        self._recv_buffer: bytearray = bytearray()
        # Decoded application messages waiting to be delivered.
        self._recv_queue: Deque[Message] = deque()
        # Callbacks waiting for a message to arrive.
        self._recv_callbacks: Deque[RecvCallback] = deque()

        # send_message calls made before the WebSocket is open: (payload_bytes, callback).
        self._pending_sends: List[tuple] = []

        # Handshake state.
        self._handshake_complete: bool = False
        self._remote_identity: Optional[bytes] = None

        # Holds JsProxy objects for callbacks so they aren't GC'd before fired.
        self._proxies: List[Any] = []

    # ------------------------------------------------------------------
    # Public API

    @classmethod
    def connect(
        cls,
        callback: ConnectCallback,
        context: IOContext,
        identity: str,
        address: str,
        max_retry_times: int = DEFAULT_MAX_RETRY_TIMES,
        init_retry_delay: int = DEFAULT_INIT_RETRY_DELAY,
    ) -> "ConnectorSocket":
        """Create a ConnectorSocket and initiate connection to the remote address.

        The callback is invoked synchronously with ``None`` to mirror the native
        semantic of "socket is ready to accept queued operations". Send/recv
        ops issued before the WebSocket actually opens are queued and dispatched
        once the WebSocket transitions to OPEN. Connection failures surface
        through the next pending recv/send callback rather than this callback.

        ``max_retry_times`` and ``init_retry_delay`` are accepted for API
        compatibility but ignored: browser WebSockets do not expose the retry
        semantics native ymq uses.
        """
        del context, max_retry_times, init_retry_delay  # unused

        ws_url = _normalize_ws_address(address)

        socket = cls(identity=identity, address=address)
        socket._open_websocket(ws_url)

        # Call back synchronously: socket is ready to accept queued ops.
        try:
            callback(None)
        except Exception:  # noqa: BLE001 — user callback errors must not corrupt our state
            pass
        return socket

    @classmethod
    def bind(cls, *args: Any, **kwargs: Any) -> "ConnectorSocket":
        raise NotImplementedError(
            "ConnectorSocket.bind is not supported in the browser; "
            "use ConnectorSocket.connect to a remote scheduler instead."
        )

    def send_message(self, callback: SendCallback, message_payload: Bytes) -> None:
        if self._closed:
            self._invoke(
                callback,
                self._close_error or _make_exception(ErrorCode.SocketStopRequested, "Socket has been shut down"),
            )
            return

        payload_bytes = b"" if message_payload.data is None else message_payload.data
        framed = struct.pack(_HEADER_FORMAT, len(payload_bytes)) + payload_bytes

        if not self._open:
            self._pending_sends.append((framed, callback))
            return

        self._raw_send(framed, callback)

    def recv_message(self, callback: RecvCallback) -> None:
        if self._recv_queue:
            msg = self._recv_queue.popleft()
            self._invoke(callback, msg)
            return

        if self._closed:
            self._invoke(
                callback,
                self._close_error or _make_exception(ErrorCode.ConnectorSocketClosedByRemoteEnd, "Socket closed"),
            )
            return

        self._recv_callbacks.append(callback)

    def shutdown(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:  # noqa: BLE001 — close on a closed socket can throw in JS
            pass
        # Drain any waiting receivers with a stop-requested error.
        err = _make_exception(ErrorCode.SocketStopRequested, "Socket has been shut down")
        while self._recv_callbacks:
            self._invoke(self._recv_callbacks.popleft(), err)
        # Fail any queued sends.
        for _framed, cb in self._pending_sends:
            self._invoke(cb, err)
        self._pending_sends.clear()
        self._proxies.clear()

    def __repr__(self) -> str:
        return f"ConnectorSocket(identity={self.identity!r}, address={self._address!r})"

    # ------------------------------------------------------------------
    # Internal: WebSocket lifecycle

    def _open_websocket(self, ws_url: str) -> None:
        # Imported lazily so that this module can be imported (and unit-tested
        # at the surface level) outside of Pyodide.
        import js  # type: ignore[import-not-found]
        from pyodide.ffi import create_proxy  # type: ignore[import-not-found]

        ws = js.WebSocket.new(ws_url)
        ws.binaryType = "arraybuffer"

        on_open = create_proxy(self._on_open)
        on_message = create_proxy(self._on_message)
        on_error = create_proxy(self._on_error)
        on_close = create_proxy(self._on_close)
        # Keep proxies alive for the lifetime of the socket.
        self._proxies.extend([on_open, on_message, on_error, on_close])

        ws.addEventListener("open", on_open)
        ws.addEventListener("message", on_message)
        ws.addEventListener("error", on_error)
        ws.addEventListener("close", on_close)

        self._ws = ws

    def _send_handshake(self) -> None:
        identity_bytes = self.identity.encode("utf-8")
        frame = _MAGIC_STRING + struct.pack(_HEADER_FORMAT, len(identity_bytes)) + identity_bytes
        # Send the handshake as a single binary frame; no callback (handshake
        # failures will surface as a close event).
        self._raw_send(frame, None)

    def _raw_send(self, data: bytes, callback: Optional[SendCallback]) -> None:
        try:
            import js  # type: ignore[import-not-found]

            buf = js.Uint8Array.new(len(data))
            # Copy bytes into the JS-side buffer via a memoryview.
            buf.assign(js.Uint8Array.new(memoryview(data)))
            self._ws.send(buf)
        except Exception as exc:  # noqa: BLE001 — surface JS errors to the caller
            if callback is not None:
                self._invoke(callback, _make_exception(ErrorCode.SysCallError, str(exc)))
            return
        if callback is not None:
            # WebSocket.send is fire-and-forget; treat as success once queued.
            self._invoke(callback, None)

    # ------------------------------------------------------------------
    # Internal: WebSocket event handlers

    def _on_open(self, _event: Any) -> None:
        if self._closed:
            return
        self._open = True
        self._send_handshake()
        # Drain queued sends.
        pending, self._pending_sends = self._pending_sends, []
        for framed, cb in pending:
            self._raw_send(framed, cb)

    def _on_message(self, event: Any) -> None:
        if self._closed:
            return
        try:
            data = event.data
            # data is a JsProxy of an ArrayBuffer (binaryType="arraybuffer").
            # to_py() converts to a bytes-like object (memoryview).
            if hasattr(data, "to_py"):
                py_data = bytes(data.to_py())
            else:
                py_data = bytes(memoryview(data))
        except Exception as exc:  # noqa: BLE001
            self._fail(_make_exception(ErrorCode.SysCallError, f"Failed to decode WS frame: {exc}"))
            return

        self._recv_buffer.extend(py_data)
        self._process_recv_buffer()

    def _on_error(self, _event: Any) -> None:
        # The browser does not expose a useful error message; details usually
        # arrive via the subsequent close event.
        if self._closed:
            return
        self._fail(_make_exception(ErrorCode.SysCallError, "WebSocket error event"))

    def _on_close(self, event: Any) -> None:
        if self._closed:
            return
        try:
            code = int(getattr(event, "code", 0))
            reason = str(getattr(event, "reason", "")) or "WebSocket closed"
        except Exception:  # noqa: BLE001
            code, reason = 0, "WebSocket closed"
        err = _make_exception(ErrorCode.ConnectorSocketClosedByRemoteEnd, f"WebSocket closed (code={code}): {reason}")
        self._fail(err)

    # ------------------------------------------------------------------
    # Internal: protocol parsing + callback dispatch

    def _process_recv_buffer(self) -> None:
        # Consume the magic string first.
        if not self._handshake_complete and self._remote_identity is None:
            if len(self._recv_buffer) < len(_MAGIC_STRING):
                return
            magic = bytes(self._recv_buffer[: len(_MAGIC_STRING)])
            if magic != _MAGIC_STRING:
                self._fail(
                    _make_exception(ErrorCode.InvalidAddressFormat, f"Invalid YMQ magic string from remote: {magic!r}")
                )
                return
            del self._recv_buffer[: len(_MAGIC_STRING)]

        # Drain framed messages.
        while True:
            if len(self._recv_buffer) < _HEADER_SIZE:
                return
            (length,) = struct.unpack_from(_HEADER_FORMAT, self._recv_buffer, 0)
            if len(self._recv_buffer) < _HEADER_SIZE + length:
                return
            payload = bytes(self._recv_buffer[_HEADER_SIZE : _HEADER_SIZE + length])
            del self._recv_buffer[: _HEADER_SIZE + length]

            if not self._handshake_complete:
                # First framed message after the magic is the remote identity.
                self._remote_identity = payload
                self._handshake_complete = True
                continue

            self._deliver_message(Message(address=None, payload=payload))

    def _deliver_message(self, message: Message) -> None:
        if self._recv_callbacks:
            self._invoke(self._recv_callbacks.popleft(), message)
        else:
            self._recv_queue.append(message)

    def _fail(self, error: Exception) -> None:
        """Mark the socket as closed and surface the error to all waiters."""
        if self._closed:
            return
        self._closed = True
        self._close_error = error
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:  # noqa: BLE001
            pass
        while self._recv_callbacks:
            self._invoke(self._recv_callbacks.popleft(), error)
        for _framed, cb in self._pending_sends:
            self._invoke(cb, error)
        self._pending_sends.clear()
        self._proxies.clear()

    @staticmethod
    def _invoke(callback: Callable[[Any], None], arg: Any) -> None:
        try:
            callback(arg)
        except Exception:  # noqa: BLE001 — never let a user callback abort the IO loop
            sys.excepthook(*sys.exc_info())  # type: ignore[misc]


# ---------------------------------------------------------------------------
# BinderSocket: explicit not-implemented stub so any accidental use fails loudly.


class BinderSocket:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError(
            "BinderSocket is not supported in the browser ymq backend; "
            "browser clients can only connect outbound to a remote scheduler."
        )


__all__ = [
    "Address",
    "AddressType",
    "BinderSocket",
    "Bytes",
    "ConnectorSocket",
    "ErrorCode",
    "IOContext",
    "Message",
    "YMQException",
    "ConnectorSocketClosedByRemoteEndError",
    "InvalidAddressFormatError",
    "InvalidPortFormatError",
    "RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError",
    "SocketStopRequestedError",
    "SysCallError",
    "DEFAULT_MAX_RETRY_TIMES",
    "DEFAULT_INIT_RETRY_DELAY",
]
