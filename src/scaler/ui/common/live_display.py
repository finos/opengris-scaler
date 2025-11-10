import dataclasses
from collections import defaultdict
from typing import Dict, List, Optional, Set

from nicegui import ui
from nicegui.element import Element

from scaler.protocol.python.common import WorkerState
from scaler.protocol.python.message import StateTask, StateWorker
from scaler.protocol.python.status import WorkerStatus
from scaler.ui.common.utility import display_capabilities, format_worker_name
from scaler.utility.formatter import format_microseconds, format_seconds


@dataclasses.dataclass
class SchedulerSection:
    cpu: str = dataclasses.field(default="")
    rss: str = dataclasses.field(default="")
    rss_free: str = dataclasses.field(default="")

    handler: Optional[Element] = dataclasses.field(default=None)

    def handle_task_state(self, _: StateTask):
        return

    def handle_worker_state(self, _: StateWorker):
        return

    def draw_section(self):
        with ui.card().classes("w-full"), ui.row() as handler:
            self.handler = handler
            ui.label("Scheduler")
            ui.label()
            ui.label("CPU:")
            ui.label().bind_text_from(self, "cpu")
            ui.label()
            ui.label("RSS:")
            ui.label().bind_text_from(self, "rss")
            ui.label()
            ui.label("RSS Free:")
            ui.label().bind_text_from(self, "rss_free")

    def delete_section(self):
        self.handler.clear()
        self.handler.delete()


@dataclasses.dataclass
class WorkerRow:
    worker: str = dataclasses.field(default="")
    agt_cpu: float = dataclasses.field(default=0)
    agt_rss: int = dataclasses.field(default=0)
    cpu: float = dataclasses.field(default=0)
    rss: int = dataclasses.field(default=0)
    rss_free: int = dataclasses.field(default=0)
    free: int = dataclasses.field(default=0)
    sent: int = dataclasses.field(default=0)
    queued: int = dataclasses.field(default=0)
    suspended: int = dataclasses.field(default=0)
    lag: str = dataclasses.field(default="")
    itl: str = dataclasses.field(default="")
    last_seen: str = dataclasses.field(default="")
    capabilities: Set[str] = dataclasses.field(default_factory=set)
    display_capabilities: str = dataclasses.field(default="")

    handlers: List[Element] = dataclasses.field(default_factory=list)

    def populate(self, data: WorkerStatus):
        self.worker = data.worker_id.decode()
        self.agt_cpu = data.agent.cpu / 10
        self.agt_rss = int(data.agent.rss / 1e6)
        self.cpu = sum(p.resource.cpu for p in data.processor_statuses) / 10
        self.rss = int(sum(p.resource.rss for p in data.processor_statuses) / 1e6)
        self.rss_free = int(data.rss_free / 1e6)
        self.free = data.free
        self.sent = data.sent
        self.queued = data.queued
        self.suspended = data.suspended
        self.lag = format_microseconds(data.lag_us)
        self.itl = data.itl
        self.last_seen = format_seconds(data.last_s)

    def set_capabilities(self, capabilities: Set[str]):
        self.capabilities = capabilities
        self.display_capabilities = display_capabilities(self.capabilities)

    def draw_row(self):
        total_rss = self.rss + self.rss_free

        ui.label(format_worker_name(self.worker))
        ui.knob(track_color="grey-2", show_value=True, min=0, max=100).bind_value_from(self, "agt_cpu")
        ui.knob(track_color="grey-2", show_value=True, min=0, max=total_rss).bind_value_from(self, "agt_rss")
        ui.knob(track_color="grey-2", show_value=True, min=0, max=100).bind_value_from(self, "cpu")
        ui.knob(track_color="grey-2", show_value=True, min=0, max=total_rss).bind_value_from(self, "rss")
        ui.label().bind_text_from(self, "free")
        ui.label().bind_text_from(self, "sent")
        ui.label().bind_text_from(self, "queued")
        ui.label().bind_text_from(self, "suspended")
        ui.label().bind_text_from(self, "lag")
        ui.label().bind_text_from(self, "itl")
        ui.label().bind_text_from(self, "last_seen")
        ui.label().bind_text_from(self, "display_capabilities")

    def delete_row(self):
        for element in self.handlers:
            element.delete()


@dataclasses.dataclass
class WorkersSection:
    workers: Dict[str, WorkerRow] = dataclasses.field(default_factory=lambda: defaultdict(WorkerRow))

    @ui.refreshable
    def draw_section(self):
        with ui.row().classes("h-max"), ui.card().classes("w-full"), ui.grid(columns=13):
            self.__draw_titles()
            for worker_row in self.workers.values():
                worker_row.draw_row()

    def handle_task_state(self, _: StateTask):
        return

    def handle_worker_state(self, state_worker: StateWorker):
        worker_id = state_worker.worker_id.decode()
        state = state_worker.state

        if state == WorkerState.Connected:
            self.workers[worker_id].set_capabilities(set(state_worker.capabilities.keys()))
        if state == WorkerState.Disconnected:
            self.workers.pop(worker_id, None)
            self.draw_section.refresh()

    @staticmethod
    def __draw_titles():
        ui.label("Worker")
        ui.label("Agt CPU %")
        ui.label("Agt RSS (in MB)")
        ui.label("Processors CPU %")
        ui.label("Processors RSS (in MB)")
        ui.label("Queue Capacity")
        ui.label("Tasks Sent")
        ui.label("Tasks Queued")
        ui.label("Tasks Suspended")
        ui.label("Lag")
        ui.label("ITL")
        ui.label("Last Seen")
        ui.label("Capabilities")
