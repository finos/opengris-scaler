import dataclasses
from threading import Lock
from typing import Dict, List, Optional

from nicegui import ui
from nicegui.element import Element

from scaler.protocol.python.common import WorkerState
from scaler.protocol.python.message import StateTask, StateWorker
from scaler.protocol.python.status import ProcessorStatus, WorkerStatus
from scaler.ui.common.utility import format_worker_name


@dataclasses.dataclass
class WorkerProcessors:
    workers: Dict[str, "WorkerProcessorTable"] = dataclasses.field(default_factory=dict)
    _lock: Lock = Lock()

    @ui.refreshable
    def draw_section(self):
        with self._lock:
            for processor_table in self.workers.values():
                processor_table.draw_table()

    def update_data(self, data: List[WorkerStatus]):
        with self._lock:
            for worker in data:
                worker_name = worker.worker_id.decode()
                processor_table = self.workers.get(worker_name)

                if processor_table is None:
                    processor_table = WorkerProcessorTable(worker_name, 0, worker.rss_free, worker.processor_statuses)
                    self.workers[worker_name] = processor_table
                elif processor_table.processor_statuses != worker.processor_statuses:
                    processor_table.processor_statuses = worker.processor_statuses

    def remove_worker(self, dead_worker: str):
        with self._lock:
            self.workers.pop(dead_worker, None)

    def handle_task_state(self, _: StateTask):
        return

    def handle_worker_state(self, state_worker: StateWorker):
        worker_id = state_worker.worker_id.decode()
        state = state_worker.state

        if state == WorkerState.Disconnected:
            self.remove_worker(worker_id)


@dataclasses.dataclass
class WorkerProcessorTable:
    worker_name: str
    rss_max: int
    rss_free: int
    processor_statuses: List[ProcessorStatus]

    handler: Optional[Element] = dataclasses.field(default=None)

    def draw_table(self):
        formatted_worker_name = format_worker_name(self.worker_name)
        with ui.card().classes("w-full") as handler:
            self.handler = handler

            ui.markdown(f"Worker **{formatted_worker_name}**").classes("text-xl")

            with ui.grid(columns=7).classes("w-full"):
                self.draw_titles()
                for processor in sorted(self.processor_statuses, key=lambda x: x.pid):
                    if processor.resource.rss > self.rss_max:
                        self.rss_max = processor.resource.rss

                    self.draw_row(processor, self.rss_free, self.rss_max)

    @staticmethod
    def draw_titles():
        ui.label("Processor PID")
        ui.label("CPU %")
        ui.label("RSS (in MB)")
        ui.label("Max RSS (in MB)")
        ui.label("Initialized")
        ui.label("Has Task")
        ui.label("Suspended")

    @staticmethod
    def draw_row(processor_status: ProcessorStatus, rss_free: int, rss_max: int):
        cpu = processor_status.resource.cpu / 10
        rss = int(processor_status.resource.rss / 1e6)
        rss_max = int(rss_max / 1e6)
        rss_free = int(rss_free / 1e6)

        ui.label(str(processor_status.pid))
        ui.knob(value=cpu, track_color="grey-2", show_value=True, min=0, max=100)
        ui.knob(value=rss, track_color="grey-2", show_value=True, min=0, max=rss + rss_free)
        ui.knob(value=rss_max, track_color="grey-2", show_value=True, min=0, max=rss + rss_free)
        ui.checkbox().bind_value_from(processor_status, "initialized")
        ui.checkbox().bind_value_from(processor_status, "has_task")
        ui.checkbox().bind_value_from(processor_status, "suspended")

    def delete_row(self):
        assert self.handler is not None
        self.handler.clear()
        self.handler.delete()
