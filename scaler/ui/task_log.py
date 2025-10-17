import dataclasses
from collections import deque
from threading import Lock
from typing import Deque, Dict, Optional

from nicegui import ui

from scaler.protocol.python.common import TaskState
from scaler.protocol.python.message import StateTask
from scaler.ui.utility import COMPLETED_TASK_STATUSES, display_capabilities
from scaler.utility.formatter import format_bytes
from scaler.utility.metadata.profile_result import ProfileResult


TASK_ID_HTML_TEMPLATE = (
    "<span "
    "style='display:inline-block;max-width:12rem;overflow:hidden;text-overflow:ellipsis;"
    "white-space:nowrap;cursor:pointer;font:inherit;color:inherit' "
    "title='{task}' onclick=\"navigator.clipboard.writeText('{task}')\">{task}</span>"
)


@dataclasses.dataclass
class TaskData:
    task: str = dataclasses.field(default="")
    function: str = dataclasses.field(default="")
    duration: str = dataclasses.field(default="")
    peak_mem: str = dataclasses.field(default="")
    status: str = dataclasses.field(default="")
    capabilities: str = dataclasses.field(default="")

    def populate(
        self,
        state: StateTask,
        function_name: str,
        profiling_data: Optional[ProfileResult],
        task_capabilities: Dict[str, int],
    ):
        self.task = f"{state.task_id.hex()}"
        self.function = function_name
        self.status = state.state.name

        if profiling_data:
            duration = profiling_data.duration_s
            mem = profiling_data.memory_peak
            self.duration = f"{duration:.2f}s"
            self.peak_mem = format_bytes(mem) if mem != 0 else "0"
        else:
            self.duration = "N/A"
            self.peak_mem = "N/A"

        self.capabilities = display_capabilities(task_capabilities)

    def draw_row(self):
        color = "color: green" if self.status == TaskState.Success.name else "color: red"
        ui.html(TASK_ID_HTML_TEMPLATE.format(task=self.task))
        ui.label(self.function)
        ui.label(self.duration)
        ui.label(self.peak_mem)
        ui.label(self.status).style(color)
        ui.label(self.capabilities)

    @staticmethod
    def draw_titles():
        ui.label("Task ID")
        ui.label("Function")
        ui.label("Duration")
        ui.label("Peak mem")
        ui.label("Status")
        ui.label("Capabilities")


class TaskLogTable:
    def __init__(self):
        self._task_log: Deque[TaskData] = deque(maxlen=100)
        self._task_id_to_function_name: Dict[str, str] = {}
        self._lock: Lock = Lock()

    def handle_task_state(self, state_task: StateTask):
        if state_task.function_name != b"" and state_task.task_id.hex() not in self._task_id_to_function_name:
            self._task_id_to_function_name[state_task.task_id.hex()] = state_task.function_name.decode()

        if state_task.state not in COMPLETED_TASK_STATUSES:
            return

        function_name = state_task.function_name.decode()
        if function_name == "":
            function_name = self._task_id_to_function_name.pop(state_task.task_id.hex(), "")

        # Canceled/failed states don't have profiling metadata
        profiling_data = ProfileResult.deserialize(state_task.metadata) if state_task.metadata != b"" else None

        row = TaskData()
        row.populate(state_task, function_name, profiling_data, state_task.capabilities)

        with self._lock:
            self._task_log.appendleft(row)

    @ui.refreshable
    def draw_section(self):
        with self._lock:
            with ui.card().classes("w-full q-mx-auto"), ui.grid(columns=6).classes("q-mx-auto"):
                TaskData.draw_titles()
                for task in self._task_log:
                    task.draw_row()
