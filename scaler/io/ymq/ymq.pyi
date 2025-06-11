# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the Ymq Python C Extension module

from enum import IntEnum

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

    def createIOSocket(self, /, identity: str, socket_type: IOSocketType) -> IOSocket: ...

class IOSocket:
    identity: str
    socket_type: IOSocketType
