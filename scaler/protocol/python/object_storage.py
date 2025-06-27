import dataclasses
import enum
import struct

from scaler.protocol.capnp._python import _object_storage  # noqa
from scaler.protocol.python.mixins import Message
from scaler.utility.identifiers import ObjectID

OBJECT_ID_FORMAT = "!QQQQ"


@dataclasses.dataclass
class ObjectRequestHeader(Message):
    class ObjectRequestType(enum.Enum):
        SetObject = _object_storage.ObjectRequestHeader.ObjectRequestType.setObject
        GetObject = _object_storage.ObjectRequestHeader.ObjectRequestType.getObject
        DeleteObject = _object_storage.ObjectRequestHeader.ObjectRequestType.deleteObject

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def object_id(self) -> ObjectID:
        return _from_capnp_object_id(self._msg.objectID)

    @property
    def payload_length(self) -> int:
        return self._msg.payloadLength

    @property
    def request_id(self) -> int:
        return self._msg.requestID

    @property
    def request_type(self) -> ObjectRequestType:
        return ObjectRequestHeader.ObjectRequestType(self._msg.requestType.raw)

    @staticmethod
    def new_msg(
        object_id: ObjectID,
        payload_length: int,
        request_id: int,
        request_type: ObjectRequestType,
    ) -> "ObjectRequestHeader":
        return ObjectRequestHeader(
            _object_storage.ObjectRequestHeader(
                objectID=_to_capnp_object_id(object_id),
                payloadLength=payload_length,
                requestID=request_id,
                requestType=request_type.value,
            )
        )

    def get_message(self):
        return self._msg


@dataclasses.dataclass
class ObjectResponseHeader(Message):
    MESSAGE_LENGTH = 80  # there does not seem to be a way to statically know the size of a pycapnp message

    class ObjectResponseType(enum.Enum):
        SetOK = _object_storage.ObjectResponseHeader.ObjectResponseType.setOK
        GetOK = _object_storage.ObjectResponseHeader.ObjectResponseType.getOK
        DelOK = _object_storage.ObjectResponseHeader.ObjectResponseType.delOK
        DelNotExists = _object_storage.ObjectResponseHeader.ObjectResponseType.delNotExists

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def object_id(self) -> ObjectID:
        return _from_capnp_object_id(self._msg.objectID)

    @property
    def payload_length(self) -> int:
        return self._msg.payloadLength

    @property
    def response_id(self) -> int:
        return self._msg.responseID

    @property
    def response_type(self) -> ObjectResponseType:
        return ObjectResponseHeader.ObjectResponseType(self._msg.responseType.raw)

    @staticmethod
    def new_msg(
        object_id: ObjectID,
        payload_length: int,
        response_id: int,
        response_type: ObjectResponseType,
    ) -> "ObjectResponseHeader":
        return ObjectResponseHeader(
            _object_storage.ObjectResponseHeader(
                objectID=_to_capnp_object_id(object_id),
                payloadLength=payload_length,
                responseID=response_id,
                responseType=response_type.value,
            )
        )

    def get_message(self):
        return self._msg


def _to_capnp_object_id(object_id: ObjectID) -> _object_storage.ObjectID:
    field0, field1, field2, field3 = struct.unpack(OBJECT_ID_FORMAT, object_id)

    return _object_storage.ObjectID(field0=field0, field1=field1, field2=field2, field3=field3)


def _from_capnp_object_id(capnp_object_id: _object_storage.ObjectID) -> ObjectID:
    return ObjectID(
        struct.pack(
            OBJECT_ID_FORMAT,
            capnp_object_id.field0,
            capnp_object_id.field1,
            capnp_object_id.field2,
            capnp_object_id.field3,
        )
    )
