import os
import socket
import uuid
from threading import Lock
from typing import Iterable, Optional

from scaler.io.mixins import SyncObjectStorageConnector
from scaler.io.ymq.ymq import IOContext, IOSocket, IOSocketType, Message, YMQException
from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.object_storage import ObjectRequestHeader, ObjectResponseHeader, to_capnp_object_id
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ObjectID

# Some OSes raise an OSError when sending buffers too large with send() or sendmsg().
MAX_CHUNK_SIZE = 128 * 1024 * 1024


class PySyncObjectStorageConnector(SyncObjectStorageConnector):
    """An synchronous connector that uses an raw TCP socket to connect to a Scaler's object storage instance."""

    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port

        self._next_request_id = 0

        self._socket_lock = Lock()

        self._identity: str = f"{os.getpid()}|{socket.gethostname().split('.')[0]}|{uuid.uuid4()}"
        self._io_context = IOContext()
        self._io_socket: IOSocket = self._io_context.createIOSocket_sync(self._identity, IOSocketType.Connector)
        self._io_socket.connect_sync(self.address)

    def __del__(self):
        self.destroy()

    def destroy(self):
        with self._socket_lock:
            if self._io_socket is not None:
                self._io_socket = None

    @property
    def address(self) -> str:
        return f"tcp://{self._host}:{self._port}"

    def set_object(self, object_id: ObjectID, payload: bytes):
        """
        Sets the object's payload on the object storage server.
        """

        with self._socket_lock:
            self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.SetObject, payload)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.SetOK])
        self.__ensure_empty_payload(response_payload)

    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytearray:
        """
        Returns the object's payload from the object storage server.

        Will block until the object is available.
        """

        with self._socket_lock:
            self.__send_request(object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.GetObject)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.GetOK])

        return response_payload

    def delete_object(self, object_id: ObjectID) -> bool:
        """
        Removes the object from the object storage server.

        Returns `False` if the object wasn't found in the server. Otherwise returns `True`.
        """

        with self._socket_lock:
            self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.DeleteObject)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(
            response_header,
            [ObjectResponseHeader.ObjectResponseType.DelOK, ObjectResponseHeader.ObjectResponseType.DelNotExists],
        )
        self.__ensure_empty_payload(response_payload)

        return response_header.response_type == ObjectResponseHeader.ObjectResponseType.DelOK

    def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        """
        Link an object's content to a new object ID on the object storage server.
        """

        object_id_payload = to_capnp_object_id(object_id).to_bytes()

        with self._socket_lock:
            self.__send_request(
                new_object_id,
                len(object_id_payload),
                ObjectRequestHeader.ObjectRequestType.DuplicateObjectID,
                object_id_payload,
            )
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.DuplicateOK])
        self.__ensure_empty_payload(response_payload)

    def __ensure_is_connected(self):
        if self._io_socket is None:
            raise ObjectStorageException("connector is closed.")

    def __ensure_response_type(
        self, header: ObjectResponseHeader, valid_response_types: Iterable[ObjectResponseHeader.ObjectResponseType]
    ):
        if header.response_type not in valid_response_types:
            raise RuntimeError(f"unexpected object storage response_type={header.response_type}.")

    def __ensure_empty_payload(self, payload: bytearray):
        if len(payload) != 0:
            raise RuntimeError(f"unexpected response payload_length={len(payload)}, expected 0.")

    def __send_request(
        self,
        object_id: ObjectID,
        payload_length: int,
        request_type: ObjectRequestHeader.ObjectRequestType,
        payload: Optional[bytes] = None,
    ):
        self.__ensure_is_connected()
        assert self._io_socket is not None

        request_id = self._next_request_id
        self._next_request_id += 1
        self._next_request_id %= 2**64 - 1  # UINT64_MAX

        header = ObjectRequestHeader.new_msg(object_id, payload_length, request_id, request_type)
        header_bytes = header.get_message().to_bytes()

        if payload is not None:
            self._io_socket.send_sync(Message(address=None, payload=header_bytes))
            self._io_socket.send_sync(Message(address=None, payload=payload))
        else:
            self._io_socket.send_sync(Message(address=None, payload=header_bytes))

    def __receive_response(self):
        assert self._io_socket is not None

        try:
            header = self.__read_response_header()
            payload = self.__read_response_payload(header)
            return header, payload
        except YMQException:
            self.__raise_connection_failure()

    def __read_response_header(self) -> ObjectResponseHeader:
        assert self._io_socket is not None

        header_bytes = self._io_socket.recv_sync().payload.data
        if header_bytes is None:
            self.__raise_connection_failure()

        # pycapnp does not like to read from a bytearray object. This look like an not-yet-resolved issue.
        # That's is annoying because it leads to an unnecessary copy of the header's buffer.
        # See https://github.com/capnproto/pycapnp/issues/153
        # header_bytes = bytes(header_bytearray)

        with _object_storage.ObjectResponseHeader.from_bytes(header_bytes) as header_message:
            return ObjectResponseHeader(header_message)

    def __read_response_payload(self, header: ObjectResponseHeader) -> bytearray:
        if header.payload_length > 0:
            res = self._io_socket.recv_sync().payload.data
            if res is None:
                self.__raise_connection_failure()
            assert len(res) == header.payload_length
            return bytearray(res)
        else:
            return bytearray()

    @staticmethod
    def __raise_connection_failure():
        raise ObjectStorageException("connection failure to object storage server.")
