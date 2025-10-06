import dataclasses
import struct
from typing import List


@dataclasses.dataclass
class TaskInfoMetadata:
    task_flags: List[str]

    def serialize(self) -> bytes:
        task_flags_encoded = [flag.encode() for flag in self.task_flags]
        task_flags_lengths = [len(flag) for flag in task_flags_encoded]

        format_string = "<I" + "".join(f"I{length}s" for length in task_flags_lengths)
        return struct.pack(
            format_string,
            len(task_flags_encoded),
            *(length for pair in zip(task_flags_lengths, task_flags_encoded) for length in pair),
        )

    @staticmethod
    def deserialize(data: bytes) -> "TaskInfoMetadata":
        offset = 0

        num_flags = struct.unpack_from("<I", data, offset)[0]
        offset += 4

        task_flags = []
        for _ in range(num_flags):
            flag_length = struct.unpack_from("<I", data, offset)[0]
            offset += 4
            flag = struct.unpack_from(f"<{flag_length}s", data, offset)[0].decode()
            offset += flag_length
            task_flags.append(flag)

        return TaskInfoMetadata(task_flags=task_flags)
