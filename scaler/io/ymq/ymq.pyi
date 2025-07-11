# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the Ymq Python C Extension module

import sys
from enum import IntEnum
from typing import SupportsBytes

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    Buffer = object

class Bytes(Buffer):
    data: bytes
    len: int

    def __init__(self, data: SupportsBytes) -> None: ...
    def __repr__(self) -> str: ...

class Message:
    address: Bytes
    payload: Bytes

    def __init__(self, address: Bytes | SupportsBytes, payload: Bytes | SupportsBytes) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

class IOSocketType(IntEnum):
    Uninit = 0
    Binder = 1
    Sub = 2
    Pub = 3
    Dealer = 4
    Router = 5
    Pair = 6

class IOContext:
    num_threads: int

    def __init__(self, num_threads: int = 1) -> None: ...
    def __repr__(self) -> str: ...

    def createIOSocket(self, /, identity: str, socket_type: IOSocketType) -> IOSocket:
        """Create an io socket with an identity and socket type"""


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

class Error(IntEnum):
    TODO = 0

class YmqException(Exception):
    error: Error

    def __init__(self, error: Error) -> None: ...
    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...
