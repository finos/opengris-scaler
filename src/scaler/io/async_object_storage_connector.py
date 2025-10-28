import asyncio
import logging
import socket
import struct
import uuid
from typing import Dict, Optional, Tuple

from scaler.io.mixins import AsyncObjectStorageConnector
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

        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

        self._next_request_id = 0
        self._pending_get_requests: Dict[ObjectID, asyncio.Future] = {}

        self._identity: bytes = (
            f"{self.__class__.__name__}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}".encode()
        )

    def __del__(self):
        if not self.is_connected():
            return

        self._writer.close()

    async def connect(self, host: str, port: int):
        self._host = host
        self._port = port

        if self.is_connected():
            raise ObjectStorageException("connector is already connected.")

        self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        await self.__read_framed_message()
        self.__write_framed(self._identity)

        try:
            await self._writer.drain()
        except ConnectionResetError:
            self.__raise_connection_failure()

        # Makes sure the socket is TCP_NODELAY. It seems to be the case by default, but that's not specified in the
        # asyncio's documentation and might change in the future.
        self._writer.get_extra_info("socket").setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        self._connected_event.set()

    async def wait_until_connected(self):
        await self._connected_event.wait()

    def is_connected(self) -> bool:
        return self._connected_event.is_set()

    async def destroy(self):
        if not self.is_connected():
            return

        if not self._writer.is_closing:
            self._writer.close()

        await self._writer.wait_closed()

    @property
    def reader(self) -> Optional[asyncio.StreamReader]:
        return self._reader

    @property
    def writer(self) -> Optional[asyncio.StreamWriter]:
        return self._writer

    @property
    def address(self) -> str:
        self.__ensure_is_connected()
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
        if self._writer is None:
            raise ObjectStorageException("connector is not connected.")

        if self._writer.is_closing():
            raise ObjectStorageException("connection is closed.")

    async def __send_request(
        self,
        object_id: ObjectID,
        payload_length: int,
        request_type: ObjectRequestHeader.ObjectRequestType,
        payload: Optional[bytes],
    ):
        self.__ensure_is_connected()
        assert self._writer is not None

        request_id = self._next_request_id
        self._next_request_id += 1
        self._next_request_id %= 2**64 - 1  # UINT64_MAX

        header = ObjectRequestHeader.new_msg(object_id, payload_length, request_id, request_type)

        self.__write_request_header(header)

        if payload is not None:
            self.__write_request_payload(payload)

        try:
            await self._writer.drain()
        except ConnectionResetError:
            self.__raise_connection_failure()

    def __write_request_header(self, header: ObjectRequestHeader):
        assert self._writer is not None
        self.__write_framed(header.get_message().to_bytes())

    def __write_request_payload(self, payload: bytes):
        assert self._writer is not None
        self.__write_framed(payload)

    async def __receive_response(self) -> Optional[Tuple[ObjectResponseHeader, bytes]]:
        assert self._reader is not None

        if self._writer.is_closing():
            return None

        try:
            header = await self.__read_response_header()
            payload = await self.__read_response_payload(header)
        except asyncio.IncompleteReadError:
            self.__raise_connection_failure()

        return header, payload

    async def __read_response_header(self) -> ObjectResponseHeader:
        assert self._reader is not None

        header_data = await self.__read_framed_message()
        assert len(header_data) == ObjectResponseHeader.MESSAGE_LENGTH

        with _object_storage.ObjectResponseHeader.from_bytes(header_data) as header_message:
            return ObjectResponseHeader(header_message)

    async def __read_response_payload(self, header: ObjectResponseHeader) -> bytes:
        assert self._reader is not None

        if header.payload_length > 0:
            res = await self.__read_framed_message()
            assert len(res) == header.payload_length
            return res
        else:
            return b""

    async def __read_framed_message(self) -> bytes:
        length_bytes = await self._reader.readexactly(8)
        (payload_length,) = struct.unpack("<Q", length_bytes)
        return await self._reader.readexactly(payload_length) if payload_length > 0 else bytes()

    def __write_framed(self, payload: bytes):
        self._writer.write(struct.pack("<Q", len(payload)))
        self._writer.write(payload)
        return

    @staticmethod
    def __raise_connection_failure():
        raise ObjectStorageException("connection failure to object storage server.")
