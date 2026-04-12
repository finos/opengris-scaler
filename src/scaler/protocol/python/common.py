import dataclasses
import enum
from typing import Tuple

import scaler.protocol.python.capnp as _common  # noqa
from scaler.protocol.python.mixins import Message
from scaler.utility.identifiers import ObjectID


class TaskResultType(enum.Enum):
    Success = _common.TaskResultType.success.value  # if submit and task is done and get result
    Failed = _common.TaskResultType.failed.value  # if submit and task is failed on worker
    FailedWorkerDied = _common.TaskResultType.failedWorkerDied.value  # if submit and worker died


class TaskCancelConfirmType(enum.Enum):
    Canceled = _common.TaskCancelConfirmType.canceled.value  # if cancel success
    CancelFailed = _common.TaskCancelConfirmType.cancelFailed.value  # if failed to cancel if task is running
    CancelNotFound = _common.TaskCancelConfirmType.cancelNotFound.value  # if try to cancel, and task is not found


class TaskTransition(enum.Enum):
    HasCapacity = _common.TaskTransition.hasCapacity.value
    TaskResultSuccess = _common.TaskTransition.taskResultSuccess.value
    TaskResultFailed = _common.TaskTransition.taskResultFailed.value
    TaskResultWorkerDied = _common.TaskTransition.taskResultWorkerDied.value
    TaskCancel = _common.TaskTransition.taskCancel.value
    TaskCancelConfirmCanceled = _common.TaskTransition.taskCancelConfirmCanceled.value
    TaskCancelConfirmFailed = _common.TaskTransition.taskCancelConfirmFailed.value
    TaskCancelConfirmNotFound = _common.TaskTransition.taskCancelConfirmNotFound.value
    BalanceTaskCancel = _common.TaskTransition.balanceTaskCancel.value
    WorkerDisconnect = _common.TaskTransition.workerDisconnect.value
    SchedulerHasTask = _common.TaskTransition.schedulerHasTask.value
    SchedulerHasNoTask = _common.TaskTransition.schedulerHasNoTask.value


class TaskState(enum.Enum):
    Inactive = _common.TaskState.inactive.value  # task is scheduled but not allocate to worker
    Running = _common.TaskState.running.value  # task is running in worker
    Canceling = _common.TaskState.canceling.value  # task is canceling state
    BalanceCanceling = _common.TaskState.balanceCanceling.value  # task is in balance canceling state
    Success = _common.TaskState.success.value  # task is finished properly
    Failed = _common.TaskState.failed.value  # task is finished but exception happened
    FailedWorkerDied = _common.TaskState.failedWorkerDied.value  # task is failed due to worker died
    Canceled = _common.TaskState.canceled.value  # task is canceled (received task cancel confirm)
    CanceledNotFound = _common.TaskState.canceledNotFound.value  # task is not found when trying to cancel
    WorkerDisconnecting = _common.TaskState.workerDisconnecting.value  # task is lost due to worker disconnecting


class WorkerState(enum.Enum):
    Connected = _common.WorkerState.connected.value
    Disconnected = _common.WorkerState.disconnected.value


@dataclasses.dataclass
class TaskCapability(Message):
    def __init__(self, msg):
        super().__init__(msg)

    @property
    def name(self) -> str:
        return self._msg.name

    @property
    def value(self) -> int:
        return self._msg.value

    @staticmethod
    def new_msg(name: str, value: int) -> "TaskCapability":
        return TaskCapability(_common.TaskCapability(name=name, value=value))

    def get_message(self):
        return self._msg


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
