import dataclasses
import struct

from scaler.protocol.python.message import Task


@dataclasses.dataclass
class TaskFlags:
    profiling: bool = dataclasses.field(default=True)
    priority: int = dataclasses.field(default=0)

    FORMAT = "!?i"

    def serialize(self) -> bytes:
        return struct.pack(TaskFlags.FORMAT, self.profiling, self.priority)

    @staticmethod
    def deserialize(data: bytes) -> "TaskFlags":
        return TaskFlags(*struct.unpack(TaskFlags.FORMAT, data))


def retrieve_task_flags_from_task(task: Task) -> TaskFlags:
    if task.metadata == b"":
        return TaskFlags()

    try:
        return TaskFlags.deserialize(task.metadata)
    except struct.error:
        raise ValueError(f"unexpected metadata value (expected {TaskFlags.__name__}).")
