from threading import Lock
from typing import Iterable, Optional

from scaler.config.types.address import AddressConfig
from scaler.io.mixins import SyncObjectStorageConnector
from scaler.io.ymq import Bytes, ConnectorSocket, IOContext, YMQException
from scaler.protocol.capnp import ObjectRequestHeader, ObjectResponseHeader
from scaler.protocol.helpers import to_capnp_object_id
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ObjectID

# Some OSes raise an OSError when sending buffers too large with send() or sendmsg().
MAX_CHUNK_SIZE = 128 * 1024 * 1024


class YMQSyncObjectStorageConnector(SyncObjectStorageConnector):
    """A synchronous connector that uses YMQ to connect to a Scaler's object storage instance."""

    def __init__(self, context: IOContext, identity: bytes, address: AddressConfig):
        self._context = context
        self._identity = identity
        self._address = address

        self._next_request_id = 0

        self._socket_lock = Lock()
        self._socket: Optional[ConnectorSocket] = None

        self._socket = ConnectorSocket.connect(self._context, self._identity.decode(), repr(self._address))

    def __del__(self):
        self.destroy()

    def destroy(self):
        with self._socket_lock:
            if self._socket is None:
                return

            self._socket.shutdown()

            self._socket = None
            self._context = None

    @property
    def address(self) -> AddressConfig:
        return self._address

    def set_object(self, object_id: ObjectID, payload: bytes):
        """
        Sets the object's payload on the object storage server.
        """

        with self._socket_lock:
            self.__send_request(object_id, len(payload), ObjectRequestHeader.ObjectRequestType.setObject, payload)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.setOK])
        self.__ensure_empty_payload(response_payload)

    def get_object(self, object_id: ObjectID, max_payload_length: int = 2**64 - 1) -> bytearray:
        """
        Returns the object's payload from the object storage server.

        Will block until the object is available.
        """

        with self._socket_lock:
            self.__send_request(object_id, max_payload_length, ObjectRequestHeader.ObjectRequestType.getObject)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.getOK])

        return response_payload

    def delete_object(self, object_id: ObjectID) -> bool:
        """
        Removes the object from the object storage server.

        Returns `False` if the object wasn't found in the server. Otherwise, returns `True`.
        """

        with self._socket_lock:
            self.__send_request(object_id, 0, ObjectRequestHeader.ObjectRequestType.deleteObject)
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(
            response_header,
            [ObjectResponseHeader.ObjectResponseType.delOK, ObjectResponseHeader.ObjectResponseType.delNotExists],
        )
        self.__ensure_empty_payload(response_payload)

        return response_header.responseType == ObjectResponseHeader.ObjectResponseType.delOK

    def duplicate_object_id(self, object_id: ObjectID, new_object_id: ObjectID) -> None:
        """
        Link an object's content to a new object ID on the object storage server.
        """

        object_id_payload = to_capnp_object_id(object_id).to_bytes()

        with self._socket_lock:
            self.__send_request(
                new_object_id,
                len(object_id_payload),
                ObjectRequestHeader.ObjectRequestType.duplicateObjectID,
                object_id_payload,
            )
            response_header, response_payload = self.__receive_response()

        self.__ensure_response_type(response_header, [ObjectResponseHeader.ObjectResponseType.duplicateOK])
        self.__ensure_empty_payload(response_payload)

    def __ensure_is_connected(self):
        if self._socket is None:
            raise ObjectStorageException("connector is closed.")

    @staticmethod
    def __ensure_response_type(
        header: ObjectResponseHeader, valid_response_types: Iterable[ObjectResponseHeader.ObjectResponseType]
    ):
        if header.responseType not in valid_response_types:
            raise RuntimeError(f"unexpected object storage response_type={header.responseType}.")

    @staticmethod
    def __ensure_empty_payload(payload: bytearray):
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
        assert self._socket is not None

        request_id = self._next_request_id
        self._next_request_id += 1
        self._next_request_id %= 2**64 - 1  # UINT64_MAX

        header = ObjectRequestHeader(
            objectID=to_capnp_object_id(object_id),
            payloadLength=payload_length,
            requestID=request_id,
            requestType=request_type,
        )
        header_bytes = header.to_bytes()

        if payload is not None:
            self._socket.send_message_sync(Bytes(header_bytes))
            self._socket.send_message_sync(Bytes(payload))
        else:
            self._socket.send_message_sync(Bytes(header_bytes))

    def __receive_response(self):
        assert self._socket is not None

        try:
            header = self.__read_response_header()
            payload = self.__read_response_payload(header)
            return header, payload
        except YMQException:
            self.__raise_connection_failure()

    def __read_response_header(self) -> ObjectResponseHeader:
        assert self._socket is not None

        header_bytes = self._socket.recv_message_sync().payload.data
        if header_bytes is None:
            self.__raise_connection_failure()

        return ObjectResponseHeader.from_bytes(header_bytes)

    def __read_response_payload(self, header: ObjectResponseHeader) -> bytearray:
        if header.payloadLength > 0:
            payload_msg = self._socket.recv_message_sync()
            res = payload_msg.payload.data
            if res is None:
                self.__raise_connection_failure()
            assert len(res) == header.payloadLength
            return bytearray(res)
        else:
            return bytearray()

    @staticmethod
    def __raise_connection_failure():
        raise ObjectStorageException("connection failure to object storage server.")
