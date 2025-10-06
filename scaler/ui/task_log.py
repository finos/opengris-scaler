import dataclasses
from collections import deque
from threading import Lock
from typing import Deque

from nicegui import ui

from scaler.protocol.python.common import TaskState
from scaler.protocol.python.message import StateTask
from scaler.ui.metadata import TaskTags
from scaler.utility.formatter import format_bytes
from scaler.utility.metadata.profile_result import ProfileResult
from scaler.utility.metadata.state_task_flags import StateTaskFlags

# TaskStatus values corresponding to completed tasks (some are in-progress e.g. Running)
COMPLETED_TASK_STATUSES = {
    TaskState.Success,
    TaskState.Failed,
    TaskState.Canceled,
    TaskState.FailedWorkerDied,
    TaskState.CanceledNotFound,
}


@dataclasses.dataclass
class TaskData:
    task: str = dataclasses.field(default="")
    function: str = dataclasses.field(default="")
    duration: str = dataclasses.field(default="")
    peak_mem: str = dataclasses.field(default="")
    status: str = dataclasses.field(default="")
    tags: str = dataclasses.field(default="")

    def populate(self, state: StateTask, profiling_data: ProfileResult, task_tags: str):
        self.task = f"{state.task_id.hex()}"
        self.function = state.function_name.decode()
        self.status = state.state.name

        duration = profiling_data.duration_s
        mem = profiling_data.memory_peak
        self.duration = f"{duration:.2f}s"
        self.peak_mem = format_bytes(mem) if mem != 0 else "0"

        self.tags = task_tags

    def draw_row(self):
        color = "color: green" if self.status == "Finished" else "color: red"
        ui.label(self.task)
        ui.label(self.function)
        ui.label(self.duration)
        ui.label(self.peak_mem)
        ui.label(self.status).style(color)
        ui.label(self.tags)

    @staticmethod
    def draw_titles():
        ui.label("Task ID")
        ui.label("Function")
        ui.label("Duration")
        ui.label("Peak mem")
        ui.label("Status")
        ui.label("Tags")


class TaskLogTable:
    def __init__(self):
        self._task_log: Deque[TaskData] = deque(maxlen=100)
        self._lock: Lock = Lock()
        self._task_tags = TaskTags()

    def handle_task_state(self, state_task: StateTask):
        if state_task.metadata == b"":
            return

        metadata = StateTaskFlags.deserialize(state_task.metadata)

        if metadata.is_task_info():
            task_info = metadata.get_task_info()
            self._task_tags.add_task_tags(state_task.task_id.hex(), task_info.task_flags)
            return

        profiling_data = metadata.get_profiling()

        row = TaskData()
        task_tags = self._task_tags.pop_task_tags(state_task.task_id.hex())
        row.populate(state_task, profiling_data, task_tags)

        with self._lock:
            self._task_log.appendleft(row)

    @ui.refreshable
    def draw_section(self):
        with self._lock:
            with ui.card().classes("w-full q-mx-auto"), ui.grid(columns=6).classes("q-mx-auto"):
                TaskData.draw_titles()
                for task in self._task_log:
                    task.draw_row()
