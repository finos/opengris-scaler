import dataclasses
import enum
from typing import Tuple

from scaler.protocol.capnp._python import _common  # noqa
from scaler.protocol.python.mixins import Message
from scaler.utility.identifiers import ObjectID


class TaskStatus(enum.Enum):
    # task is accepted by scheduler, but will have below status
    Success = _common.TaskStatus.success  # if submit and task is done and get result
    Failed = _common.TaskStatus.failed  # if submit and task is failed on worker
    Canceled = _common.TaskStatus.canceled  # if submit and task is canceled
    NotFound = _common.TaskStatus.notFound  # if submit and task is not found in scheduler
    WorkerDied = (
        _common.TaskStatus.workerDied
    )  # if submit and worker died (only happened when scheduler keep_task=False)
    NoWorker = _common.TaskStatus.noWorker  # if submit and scheduler is full (not implemented yet)

    # below are only used for monitoring channel, not sent to client
    Inactive = _common.TaskStatus.inactive  # task is scheduled but not allocate to worker
    Running = _common.TaskStatus.running  # task is running in worker
    Canceling = _common.TaskStatus.canceling  # task is canceling (can be in Inactive or Running state)


@dataclasses.dataclass
class ObjectMetadata(Message):
    class ObjectContentType(enum.Enum):
        # FIXME: Pycapnp does not support assignment of raw enum values when the enum is itself declared within a list.
        # However, assigning the enum's string value works.
        # See https://github.com/capnproto/pycapnp/issues/374

        Serializer = "serializer"
        Object = "object"

    def __init__(self, msg):
        super().__init__(msg)

    @property
    def object_ids(self) -> Tuple[ObjectID, ...]:
        return tuple(ObjectID(object_id_bytes) for object_id_bytes in self._msg.objectIds)

    @property
    def object_types(self) -> Tuple[ObjectContentType, ...]:
        return tuple(ObjectMetadata.ObjectContentType(object_type._as_str()) for object_type in self._msg.objectTypes)

    @property
    def object_names(self) -> Tuple[bytes, ...]:
        return tuple(self._msg.objectNames)

    @staticmethod
    def new_msg(
        object_ids: Tuple[ObjectID, ...],
        object_types: Tuple[ObjectContentType, ...] = tuple(),
        object_names: Tuple[bytes, ...] = tuple(),
    ) -> "ObjectMetadata":
        return ObjectMetadata(
            _common.ObjectMetadata(
                objectIds=[bytes(object_id) for object_id in object_ids],
                objectTypes=[object_type.value for object_type in object_types],
                objectNames=list(object_names),
            )
        )

    def get_message(self):
        return self._msg


@dataclasses.dataclass
class ObjectStorageAddress(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def host(self) -> str:
        return self._msg.host

    @property
    def port(self) -> int:
        return self._msg.port

    @staticmethod
    def new_msg(host: str, port: int) -> "ObjectStorageAddress":
        return ObjectStorageAddress(_common.ObjectStorageAddress(host=host, port=port))

    def get_message(self):
        return self._msg

    def __repr__(self) -> str:
        return f"tcp://{self.host}:{self.port}"
