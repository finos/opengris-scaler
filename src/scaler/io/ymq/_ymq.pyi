# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the Ymq Python C Extension module
from enum import IntEnum
from typing import Callable, Optional, SupportsBytes, Union

try:
    from collections.abc import Buffer  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import Buffer

class Bytes(Buffer):
    data: bytes | None
    len: int

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
    def __str__(self) -> str: ...

class IOSocketType(IntEnum):
    Uninit = 0
    Binder = 1
    Connector = 2
    Unicast = 3
    Multicast = 4

class BaseIOContext:
    num_threads: int

    def __init__(self, num_threads: int = 1) -> None: ...
    def __repr__(self) -> str: ...
    def createIOSocket(
        self, callback: Callable[[Union[BaseIOSocket, Exception]], None], identity: str, socket_type: IOSocketType
    ) -> None:
        """Create an io socket with an identity and socket type"""

class BaseIOSocket:
    identity: str
    socket_type: IOSocketType

    def __repr__(self) -> str: ...
    def send(self, callback: Callable[[Optional[Exception]], None], message: Message) -> None:
        """Send a message to one of the socket's peers"""

    def recv(self, callback: Callable[[Union[Message, Exception]], None]) -> None:
        """Receive a message from one of the socket's peers"""

    def bind(self, callback: Callable[[Optional[Exception]], None], address: str) -> None:
        """Bind the socket to an address and listen for incoming connections"""

    def connect(self, callback: Callable[[Optional[Exception]], None], address: str) -> None:
        """Connect to a remote socket"""

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

    def explanation(self) -> str: ...

class YMQException(Exception):
    code: ErrorCode
    message: str

    def __init__(self, /, code: ErrorCode, message: str) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
