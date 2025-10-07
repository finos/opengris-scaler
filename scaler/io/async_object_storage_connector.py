import asyncio
import logging
import os
import socket
import uuid
from typing import Dict, Optional, Tuple

from scaler.io.mixins import AsyncObjectStorageConnector
from scaler.io.ymq.ymq import IOSocketType, IOContext, Message, YMQException
from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.object_storage import ObjectRequestHeader, ObjectResponseHeader, to_capnp_object_id
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ObjectID


class PyAsyncObjectStorageConnector(AsyncObjectStorageConnector):
    """An asyncio connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(self):
        self._host: Optional[str] = None
        self._port: Optional[int] = None

        self._connected_event = asyncio.Event()

        self._next_request_id = 0
        self._pending_get_requests: Dict[ObjectID, asyncio.Future] = {}

        self._lock = asyncio.Lock()
        self._identity: str = f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}"
        self._io_context: IOContext = IOContext()
        self._io_socket = self._io_context.createIOSocket_sync(self._identity, IOSocketType.Connector)

    def __del__(self):
        if not self.is_connected():
            return
        self._io_socket = None

    async def connect(self, host: str, port: int):
        self._host = host
        self._port = port

        if self.is_connected():
            raise ObjectStorageException("connector is already connected.")
        await self._io_socket.connect(self.address)
        self._connected_event.set()

    async def wait_until_connected(self):
        await self._connected_event.wait()

    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    async def destroy(self):
        if not self.is_connected():
            return
        self._io_socket = None

    @property
    def address(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    async def routine(self):
        await self.wait_until_connected()

        response = await self.__receive_response()
        if response is None:
            return

        header, payload = response

        if header.response_type != ObjectResponseHeader.ObjectResponseType.GetOK:
            return

        pending_get_future = self._pending_get_requests.pop(header.object_id, None)

        if pending_get_future is None:
            logging.warning(f"unknown get-ok response for unrequested object_id={repr(header.object_id)}.")
            return

        pending_get_future.set_result(payload)

    async def set_object(self, object_id: ObjectID, payload: bytes) -> None:
        await self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.SetObject, payload)

    async def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytes:
        pending_get_future = self._pending_get_requests.get(object_id)

        if pending_get_future is None:
            pending_get_future = asyncio.Future()
            self._pending_get_requests[object_id] = pending_get_future

            await self.__send_request(
                object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.GetObject, None
            )

        return await pending_get_future

    async def delete_object(self, object_id: ObjectID) -> None:
        await self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.DeleteObject, None)

    async def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        object_id_payload = to_capnp_object_id(object_id).to_bytes()

        await self.__send_request(
            new_object_id,
            len(object_id_payload),
            ObjectRequestHeader.ObjectRequestType.DuplicateObjectID,
            object_id_payload,
        )

    def __ensure_is_connected(self):
        if self._io_socket is None:
            raise ObjectStorageException("connector is not connected.")

    async def __send_request(
        self,
        object_id: ObjectID,
        payload_length: int,
        request_type: ObjectRequestHeader.ObjectRequestType,
        payload: Optional[bytes],
    ):
        self.__ensure_is_connected()

        request_id = self._next_request_id
        self._next_request_id += 1
        self._next_request_id %= 2**64 - 1  # UINT64_MAX

        header = ObjectRequestHeader.new_msg(object_id, payload_length, request_id, request_type)

        try:
            async with self._lock:
                await self.__write_request_header(header)

                if payload is not None:
                    await self.__write_request_payload(payload)

        except YMQException:
            self._io_socket = None
            self.__raise_connection_failure()

    async def __write_request_header(self, header: ObjectRequestHeader):
        assert self._io_socket is not None
        await self._io_socket.send(Message(address=None, payload=header.get_message().to_bytes()))

    async def __write_request_payload(self, payload: bytes):
        assert self._io_socket is not None
        await self._io_socket.send(Message(address=None, payload=payload))

    async def __receive_response(self) -> Optional[Tuple[ObjectResponseHeader, bytes]]:
        if self._io_socket is None:
            return None

        try:
            header = await self.__read_response_header()
            payload = await self.__read_response_payload(header)
        except YMQException:
            self._io_socket = None
            self.__raise_connection_failure()

        return header, payload

    async def __read_response_header(self) -> ObjectResponseHeader:
        assert self._io_socket is not None

        msg = await self._io_socket.recv()
        header_data = msg.payload.data
        assert len(header_data) == ObjectResponseHeader.MESSAGE_LENGTH

        with _object_storage.ObjectResponseHeader.from_bytes(header_data) as header_message:
            return ObjectResponseHeader(header_message)

    async def __read_response_payload(self, header: ObjectResponseHeader) -> bytes:
        assert self._io_socket is not None
        # assert self._reader is not None

        if header.payload_length > 0:
            res = await self._io_socket.recv()
            assert len(res.payload) == header.payload_length
            return res.payload.data
        else:
            return b""

    @staticmethod
    def __raise_connection_failure():
        raise ObjectStorageException("connection failure to object storage server.")
