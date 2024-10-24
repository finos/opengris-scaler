import dataclasses
import enum
from typing import List, Tuple

from scaler.protocol.capnp._python import _common  # noqa
from scaler.protocol.python.mixins import Message


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
class ObjectContent(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def object_ids(self) -> Tuple[bytes, ...]:
        return tuple(self._msg.objectIds)

    @property
    def object_names(self) -> Tuple[bytes, ...]:
        return tuple(self._msg.objectNames)

    @property
    def object_bytes(self) -> Tuple[List[bytes], ...]:
        return tuple(self._msg.objectBytes)

    @staticmethod
    def new_msg(
        object_ids: Tuple[bytes, ...],
        object_names: Tuple[bytes, ...] = tuple(),
        object_bytes: Tuple[List[bytes], ...] = tuple(),
    ) -> "ObjectContent":
        return ObjectContent(
            _common.ObjectContent(
                objectIds=list(object_ids), objectNames=list(object_names), objectBytes=tuple(object_bytes)
            )
        )

    def get_message(self):
        return self._msg
