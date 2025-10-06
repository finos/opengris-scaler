import dataclasses
import struct
from typing import Union
import enum

from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.metadata.task_info_metadata import TaskInfoMetadata


class StateTaskFlagsType(enum.Enum):
    PROFILING = 1
    TASK_INFO = 2


@dataclasses.dataclass
class StateTaskFlags:
    type: StateTaskFlagsType
    data: Union[ProfileResult, TaskInfoMetadata]

    FORMAT = "!I"  # For the type enum

    def serialize(self) -> bytes:
        type_bytes = struct.pack(self.FORMAT, self.type.value)
        data_bytes = self.data.serialize()

        return type_bytes + data_bytes

    @staticmethod
    def deserialize(data: bytes) -> "StateTaskFlags":
        type_value = struct.unpack(StateTaskFlags.FORMAT, data[:4])[0]
        remaining_data = data[4:]

        try:
            type_enum = StateTaskFlagsType(type_value)
        except ValueError:
            raise ValueError(f"Unknown StateTaskFlags type value: {type_value}")

        if type_enum == StateTaskFlagsType.PROFILING:
            deserialized_data = ProfileResult.deserialize(remaining_data)
        elif type_enum == StateTaskFlagsType.TASK_INFO:
            deserialized_data = TaskInfoMetadata.deserialize(remaining_data)
        else:
            raise ValueError(f"Unhandled StateTaskFlags type: {type_enum}")

        return StateTaskFlags(type=type_enum, data=deserialized_data)

    @classmethod
    def from_profiling(cls, profile_result: ProfileResult) -> "StateTaskFlags":
        return cls(type=StateTaskFlagsType.PROFILING, data=profile_result)

    @classmethod
    def from_profiling_bytes(cls, profiling_bytes: bytes) -> "StateTaskFlags":
        profile_result = ProfileResult.deserialize(profiling_bytes)
        return cls(type=StateTaskFlagsType.PROFILING, data=profile_result)

    @classmethod
    def from_task_info(cls, task_info: TaskInfoMetadata) -> "StateTaskFlags":
        return cls(type=StateTaskFlagsType.TASK_INFO, data=task_info)

    @classmethod
    def from_task_info_bytes(cls, task_info_bytes: bytes) -> "StateTaskFlags":
        task_info = TaskInfoMetadata.deserialize(task_info_bytes)
        return cls(type=StateTaskFlagsType.TASK_INFO, data=task_info)

    def is_profiling(self) -> bool:
        return self.type == StateTaskFlagsType.PROFILING

    def is_task_info(self) -> bool:
        return self.type == StateTaskFlagsType.TASK_INFO

    def get_profiling(self) -> ProfileResult:
        if not self.is_profiling():
            raise ValueError("StateTaskFlags does not contain profiling data")

        return self.data

    def get_task_info(self) -> TaskInfoMetadata:
        if not self.is_task_info():
            raise ValueError("StateTaskFlags does not contain task info data")

        return self.data
