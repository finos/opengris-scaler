import dataclasses
import struct
from typing import Optional

from scaler.protocol.python.message import TaskResult


@dataclasses.dataclass
class ProfileResult:
    duration_s: float = dataclasses.field(default=0.0)
    memory_peak: int = dataclasses.field(default=0)
    cpu_time_s: float = dataclasses.field(default=0.0)

    FORMAT = "!fQf"  # duration, memory peak, CPU time

    def serialize(self) -> bytes:
        return struct.pack(self.FORMAT, self.duration_s, self.memory_peak, self.cpu_time_s)

    @staticmethod
    def deserialize(data: bytes) -> "ProfileResult":
        return ProfileResult(*struct.unpack(ProfileResult.FORMAT, data))


def retrieve_profiling_result_from_task_result(task_result: TaskResult) -> Optional[ProfileResult]:
    if task_result.metadata == b"":
        return None

    try:
        return ProfileResult.deserialize(task_result.metadata)
    except struct.error:
        raise ValueError(f"unexpected metadata value (expected {ProfileResult.__name__}).")
