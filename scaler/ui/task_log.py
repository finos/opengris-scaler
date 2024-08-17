import dataclasses
from collections import deque
from threading import Lock
from typing import Deque

from nicegui import ui

from scaler.protocol.python.common import TaskStatus
from scaler.protocol.python.message import StateTask
from scaler.utility.formatter import format_bytes
from scaler.utility.metadata.profile_result import ProfileResult

# TaskStatus values corresponding to completed tasks (some are in-progress e.g. Running)
COMPLETED_TASK_STATUSES = {
    TaskStatus.Success,
    TaskStatus.Failed,
    TaskStatus.Canceled,
    TaskStatus.WorkerDied,
    TaskStatus.NoWorker,
}


@dataclasses.dataclass
class TaskData:
    task: str = dataclasses.field(default="")
    function: str = dataclasses.field(default="")
    duration: str = dataclasses.field(default="")
    peak_mem: str = dataclasses.field(default="")
    status: str = dataclasses.field(default="")

    def populate(self, state: StateTask):
        self.task = f"{state.task_id.hex()}"
        self.function = state.function_name.decode()
        self.status = state.status.name

        self.duration = "N/A"
        self.peak_mem = "N/A"
        if state.metadata:
            profiling_data = ProfileResult.deserialize(state.metadata)
            duration = profiling_data.duration_s
            mem = profiling_data.memory_peak
            self.duration = f"{duration:.2f}s"
            self.peak_mem = format_bytes(mem) if mem != 0 else "0"

    def draw_row(self):
        color = "color: green" if self.status == "Success" else "color: red"
        ui.label(self.task)
        ui.label(self.function)
        ui.label(self.duration)
        ui.label(self.peak_mem)
        ui.label(self.status).style(color)

    @staticmethod
    def draw_titles():
        ui.label("Task ID")
        ui.label("Function")
        ui.label("Duration")
        ui.label("Peak mem")
        ui.label("Status")


class TaskLogTable:
    def __init__(self):
        self._task_log: Deque[TaskData] = deque(maxlen=100)
        self._lock = Lock()

    def handle_task_state(self, state: StateTask):
        if state.status not in COMPLETED_TASK_STATUSES:
            return

        row = TaskData()
        row.populate(state)

        with self._lock:
            self._task_log.appendleft(row)

    @ui.refreshable
    def draw_section(self):
        with self._lock:
            with ui.card().classes("w-full q-mx-auto"), ui.grid(columns=5).classes("q-mx-auto"):
                TaskData.draw_titles()
                for task in self._task_log:
                    task.draw_row()
