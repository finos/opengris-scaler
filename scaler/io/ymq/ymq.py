# This file wraps the interface exported by the C implementation of the module
# and provides a more ergonomic interface supporting both asynchronous and synchronous execution

__all__ = ["IOSocket", "IOContext", "Message", "IOSocketType", "YMQException", "Bytes", "ErrorCode"]

import asyncio
import concurrent.futures
from typing import Optional, Callable, ParamSpec, TypeVar, Union, Concatenate

from scaler.io.ymq._ymq import BaseIOSocket, Message, IOSocketType, BaseIOContext, YMQException, Bytes, ErrorCode


class IOSocket:
    _base: BaseIOSocket

    def __init__(self, base: BaseIOSocket) -> None:
        self._base = base

    @property
    def socket_type(self) -> IOSocketType:
        return self._base.socket_type

    @property
    def identity(self) -> str:
        return self._base.identity

    async def bind(self, address: str) -> None:
        """Bind the socket to an address and listen for incoming connections"""
        await call_async(self._base.bind, address)

    def bind_sync(self, address: str, /, timeout: Optional[float] = None) -> None:
        """Bind the socket to an address and listen for incoming connections"""
        call_sync(self._base.bind, address, timeout=timeout)

    async def connect(self, address: str) -> None:
        """Connect to a remote socket"""
        await call_async(self._base.connect, address)

    def connect_sync(self, address: str, /, timeout: Optional[float] = None) -> None:
        """Connect to a remote socket"""
        call_sync(self._base.connect, address, timeout=timeout)

    async def send(self, message: Message) -> None:
        """Send a message to one of the socket's peers"""
        await call_async(self._base.send, message)

    def send_sync(self, message: Message, /, timeout: Optional[float] = None) -> None:
        """Send a message to one of the socket's peers"""
        call_sync(self._base.send, message, timeout=timeout)

    async def recv(self) -> Message:
        """Receive a message from one of the socket's peers"""
        return await call_async(self._base.recv)

    def recv_sync(self, /, timeout: Optional[float] = None) -> Message:
        """Receive a message from one of the socket's peers"""
        return call_sync(self._base.recv, timeout=timeout)


class IOContext:
    _base: BaseIOContext

    def __init__(self, num_threads: int = 1) -> None:
        self._base = BaseIOContext(num_threads)

    async def createIOSocket(self, identity: str, socket_type: IOSocketType) -> IOSocket:
        """Create an io socket with an identity and socket type"""
        return IOSocket(await call_async(self._base.createIOSocket, identity, socket_type))

    def createIOSocket_sync(self, identity: str, socket_type: IOSocketType) -> IOSocket:
        """Create an io socket with an identity and socket type"""
        return IOSocket(call_sync(self._base.createIOSocket, identity, socket_type))


P = ParamSpec("P")
T = TypeVar("T")


async def call_async(
    func: Callable[Concatenate[Callable[[Union[T, Exception]], None], P], None], *args: P.args, **kwargs: P.kwargs
) -> T:
    future = asyncio.get_event_loop().create_future()

    def callback(result: Union[T, Exception]):
        if future.done():
            return
        
        loop = future.get_loop()

        if isinstance(result, Exception):
            loop.call_soon_threadsafe(future.set_exception, result)
        else:
            loop.call_soon_threadsafe(future.set_result, result)


    func(callback, *args, **kwargs)
    return await future


def call_sync(  # type: ignore[valid-type]
    func: Callable[Concatenate[Callable[[Union[T, Exception]], None], P], None],
    *args: P.args,
    timeout: Optional[float] = None,
    **kwargs: P.kwargs,
) -> T:
    future: concurrent.futures.Future = concurrent.futures.Future()

    def callback(result: Union[T, Exception]):
        if future.done():
            return

        if isinstance(result, Exception):
            future.set_exception(result)
        else:
            future.set_result(result)

    func(callback, *args, **kwargs)
    return future.result(timeout)
