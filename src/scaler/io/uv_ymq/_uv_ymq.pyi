# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the UV YMQ Python C Extension module

from enum import IntEnum
from typing import Callable, Optional, SupportsBytes, Union

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer

class Bytes(Buffer):
    data: bytes | None
    """Data of the Bytes object"""

    len: int
    """Length of the Bytes object"""

    def __init__(self, data: Buffer | None = None) -> None: ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...

    # this type signature is not 100% accurate because it's implemented in C
    # but this satisfies the type check and is good enough
    def __buffer__(self, flags: int, /) -> memoryview: ...

class Message:
    address: Bytes | None
    payload: Bytes

    def __init__(
        self, address: Bytes | bytes | SupportsBytes | None, payload: Bytes | bytes | SupportsBytes
    ) -> None: ...
    def __repr__(self) -> str: ...

class AddressType(IntEnum):
    """Address type enum"""

    IPC = 0
    TCP = 1

class Address:
    """
    A socket address, can either be a TCP address (IPv4/6) or an IPC path.

    Example address strings:
        - ipc://some_ipc_socket_name
        - tcp://127.0.0.1:1827
        - tcp://[2001:db8::1]:1211
    """

    type: AddressType
    """Get the address type (IPC or TCP)"""

    def __init__(self, address: str) -> None:
        """Create an Address from a string."""

    def __repr__(self) -> str: ...

class IOContext:
    """Manages a pool of IO event threads"""

    num_threads: int
    """Get the number of threads in the IOContext"""

    def __init__(self, num_threads: int = 1) -> None:
        """Create an IOContext with the specified number of threads"""

    def __repr__(self) -> str: ...

class BinderSocket:
    """A binder socket that can bind to an address and communicate with multiple peers"""

    identity: str
    """Get the identity of the socket"""

    def __init__(self, context: IOContext, identity: str) -> None:
        """Create a BinderSocket with the specified identity."""

    def __repr__(self) -> str: ...

    def bind_to(self, callback: Callable[[Union[Address, Exception]], None], address: str) -> None:
        """Bind the socket to an address and listen for incoming connections."""

    def send_message(
        self, on_message_send: Callable[[Optional[Exception]], None], remote_identity: str, message_payload: Bytes
    ) -> None:
        """Send a message to a remote peer."""

    def recv_message(self, callback: Callable[[Union[Message, Exception]], None]) -> None:
        """Receive a message from a remote peer."""

    def close_connection(self, remote_identity: str) -> None:
        """Close the connection to a specific remote peer."""

class ErrorCode(IntEnum):
    Uninit = 0
    InvalidPortFormat = 1
    InvalidAddressFormat = 2
    ConfigurationError = 3
    SignalNotSupported = 4
    CoreBug = 5
    RepetetiveIOSocketIdentity = 6
    RedundantIOSocketRefCount = 7
    MultipleConnectToNotSupported = 8
    MultipleBindToNotSupported = 9
    InitialConnectFailedWithInProgress = 10
    SendMessageRequestCouldNotComplete = 11
    SetSockOptNonFatalFailure = 12
    IPv6NotSupported = 13
    RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery = 14
    ConnectorSocketClosedByRemoteEnd = 15
    IOSocketStopRequested = 16
    BinderSendMessageWithNoAddress = 17
    IPCOnWinNotSupported = 18
    UVError = 19

    def explanation(self) -> str: ...

class UVYMQException(Exception):
    code: ErrorCode
    """Error code"""

    message: str
    """Error message"""

    def __init__(self, /, code: ErrorCode, message: str) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class InvalidPortFormatError(UVYMQException): ...
class InvalidAddressFormatError(UVYMQException): ...
class ConfigurationError(UVYMQException): ...
class SignalNotSupportedError(UVYMQException): ...
class CoreBugError(UVYMQException): ...
class RepetetiveIOSocketIdentityError(UVYMQException): ...
class RedundantIOSocketRefCountError(UVYMQException): ...
class MultipleConnectToNotSupportedError(UVYMQException): ...
class MultipleBindToNotSupportedError(UVYMQException): ...
class InitialConnectFailedWithInProgressError(UVYMQException): ...
class SendMessageRequestCouldNotCompleteError(UVYMQException): ...
class SetSockOptNonFatalFailureError(UVYMQException): ...
class IPv6NotSupportedError(UVYMQException): ...
class RemoteEndDisconnectedOnSocketWithoutGuaranteedDeliveryError(UVYMQException): ...
class ConnectorSocketClosedByRemoteEndError(UVYMQException): ...
class IOSocketStopRequestedError(UVYMQException): ...
class BinderSendMessageWithNoAddressError(UVYMQException): ...
class IPCOnWinNotSupportedError(UVYMQException): ...
class UVError(UVYMQException): ...
