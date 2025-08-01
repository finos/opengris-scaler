# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the Ymq Python C Extension module

import sys
from enum import IntEnum
from typing import SupportsBytes
from collections.abc import Awaitable

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = object

class Bytes(Buffer):
    data: bytes
    len: int

    def __init__(self, data: SupportsBytes | bytes) -> None: ...
    def __repr__(self) -> str: ...

class Message:
    address: Bytes
    payload: Bytes

    def __init__(self, address: Bytes | bytes | SupportsBytes | None, payload: Bytes | bytes | SupportsBytes) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class IOSocketType(IntEnum):
    Uninit = 0
    Binder = 1
    Connector = 2
    Unicast = 3
    Multicast = 4

class IOContext:
    num_threads: int

    def __init__(self, num_threads: int = 1) -> None: ...
    def __repr__(self) -> str: ...

    def createIOSocket(self, /, identity: str, socket_type: IOSocketType) -> Awaitable[IOSocket]:
        """Create an io socket with an identity and socket type"""

    def createIOSocket_sync(self, /, identity: str, socket_type: IOSocketType) -> IOSocket:
        """Create an io socket with an identity and socket type synchronously"""


class IOSocket:
    identity: str
    socket_type: IOSocketType

    def __repr__(self) -> str: ...

    async def send(self, message: Message) -> None:
        """Send a message to one of the socket's peers"""
    async def recv(self) -> Message:
        """Receive a message from one of the socket's peers"""
    async def bind(self, address: str) -> None:
        """Bind the socket to an address and listen for incoming connections"""
    async def connect(self, address: str) -> None:
        """Connect to a remote socket"""

    def send_sync(self, message: Message) -> None:
        """Send a message to one of the socket's peers synchronously"""
    def recv_sync(self) -> Message:
        """Receive a message from one of the socket's peers synchronously"""
    def bind_sync(self, address: str) -> None:
        """Bind the socket to an address and listen for incoming connections synchronously"""
    def connect_sync(self, address: str) -> None:
        """Connect to a remote socket synchronously"""

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

    def explanation(self) -> str: ...

class YMQException(Exception):
    code: ErrorCode
    message: str

    def __init__(self, code: ErrorCode, message: str) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
