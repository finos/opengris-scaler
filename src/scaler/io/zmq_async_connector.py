import logging
from typing import Awaitable, Callable, Optional

import zmq
import zmq.asyncio

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType
from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import BaseMessage


class ZMQAsyncConnector(AsyncConnector):
    def __init__(
        self, context: zmq.asyncio.Context, identity: bytes, callback: Callable[[BaseMessage], Awaitable[None]]
    ):
        self._context = context
        self._identity = identity
        self._address: Optional[AddressConfig] = None
        self._socket: Optional[zmq.asyncio.Socket] = None

        self._callback: Callable[[BaseMessage], Awaitable[None]] = callback

    def __del__(self):
        self.destroy()

    def destroy(self):
        if self._socket is None:
            return

        if self._socket.closed:
            return

        self._socket.close(linger=1)

    async def connect(self, address: AddressConfig, remote_type: ConnectorRemoteType) -> None:
        self._address = address
        self.__create_socket(remote_type)
        assert self._socket is not None

        self._socket.connect(repr(self._address))

    async def bind(self, address: AddressConfig) -> None:
        self.__create_socket(ConnectorRemoteType.Connector)
        assert self._socket is not None

        self._socket.bind(repr(address))

        endpoint = self._socket.getsockopt(zmq.LAST_ENDPOINT)
        assert isinstance(endpoint, bytes)
        self._address = AddressConfig.from_string(endpoint.decode())

    @property
    def identity(self) -> bytes:
        return self._identity

    @property
    def socket(self) -> zmq.asyncio.Socket:
        assert self._socket is not None
        return self._socket

    @property
    def address(self) -> Optional[AddressConfig]:
        return self._address

    async def routine(self):
        message: Optional[BaseMessage] = await self.receive()
        if message is None:
            return

        await self._callback(message)

    async def receive(self) -> Optional[BaseMessage]:
        if self._context.closed:
            return None

        if self._socket is None:
            return None

        if self._socket.closed:
            return None

        payload = await self._socket.recv(copy=False)
        result: Optional[BaseMessage] = deserialize(payload.bytes)
        if result is None:
            logging.error(f"received unknown message: {payload.bytes!r}")
            return None

        return result

    async def send(self, message: BaseMessage):
        if self._socket is None:
            return

        await self._socket.send(serialize(message), copy=False)

    def __create_socket(self, remote_type: ConnectorRemoteType) -> None:
        assert self._socket is None

        if remote_type == ConnectorRemoteType.Connector:
            socket_type = zmq.PAIR
        elif remote_type == ConnectorRemoteType.Binder:
            socket_type = zmq.DEALER
        else:
            raise ValueError(f"unsupported remote_type={remote_type}")

        self._socket = self._context.socket(socket_type)
        assert self._socket is not None

        self._socket.setsockopt(zmq.IDENTITY, self._identity)
        self._socket.setsockopt(zmq.SNDHWM, 0)
        self._socket.setsockopt(zmq.RCVHWM, 0)
