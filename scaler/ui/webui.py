import dataclasses
import threading
from functools import partial

from nicegui import ui

from scaler.io.sync_subscriber import SyncSubscriber
from scaler.protocol.python.message import StateScheduler, StateTask
from scaler.protocol.python.mixins import Message
from scaler.ui.live_display import SchedulerSection, WorkersSection
from scaler.ui.memory_window import MemoryChart
from scaler.ui.setting_page import Settings
from scaler.ui.task_graph import TaskStream
from scaler.ui.task_log import TaskLogTable
from scaler.ui.worker_processors import WorkerProcessors
from scaler.utility.formatter import format_bytes, format_percentage
from scaler.utility.zmq_config import ZMQConfig


@dataclasses.dataclass
class Sections:
    scheduler_section: SchedulerSection
    workers_section: WorkersSection
    task_stream_section: TaskStream
    memory_usage_section: MemoryChart
    tasklog_section: TaskLogTable
    worker_processors: WorkerProcessors
    settings_section: Settings


def start_webui(address: str, host: str, port: int):
    tables = Sections(
        scheduler_section=SchedulerSection(),
        workers_section=WorkersSection(),
        task_stream_section=TaskStream(),
        memory_usage_section=MemoryChart(),
        tasklog_section=TaskLogTable(),
        worker_processors=WorkerProcessors(),
        settings_section=Settings(),
    )

    with ui.tabs().classes("w-full h-full") as tabs:
        live_tab = ui.tab("Live")
        tasklog_tab = ui.tab("Task Log")
        stream_tab = ui.tab("Worker Task Stream")
        worker_processors_tab = ui.tab("Worker Processors")
        settings_tab = ui.tab("Settings")

    with ui.tab_panels(tabs, value=live_tab).classes("w-full"):
        with ui.tab_panel(live_tab):
            tables.scheduler_section.draw_section()
            tables.workers_section.draw_section()

        with ui.tab_panel(tasklog_tab):
            tables.tasklog_section.draw_section()
            ui.timer(0.5, tables.tasklog_section.draw_section.refresh, active=True)
            pass

        with ui.tab_panel(stream_tab):
            tables.task_stream_section.setup_task_stream(tables.settings_section)
            ui.timer(0.1, tables.task_stream_section.update_plot, active=True)

            tables.memory_usage_section.setup_memory_chart(tables.settings_section)
            ui.timer(0.1, tables.memory_usage_section.update_plot, active=True)

        with ui.tab_panel(worker_processors_tab) as tab_handler:
            tables.worker_processors.draw_section(tab_handler)

        with ui.tab_panel(settings_tab):
            tables.settings_section.draw_section()

    subscriber = SyncSubscriber(
        address=ZMQConfig.from_string(address),
        callback=partial(__show_status, tables=tables),
        topic=b"",
        timeout_seconds=-1,
    )
    subscriber.start()

    ui_thread = threading.Thread(target=partial(ui.run, host=host, port=port, reload=False), daemon=False)
    ui_thread.start()


def __show_status(status: Message, tables: Sections):
    if isinstance(status, StateScheduler):
        __update_scheduler_state(status, tables)
        return

    if isinstance(status, StateTask):
        tables.task_stream_section.handle_task_state(status)
        tables.memory_usage_section.handle_task_state(status)
        tables.tasklog_section.handle_task_state(status)
        return


def __update_scheduler_state(data: StateScheduler, tables: Sections):
    tables.scheduler_section.cpu = format_percentage(data.scheduler.cpu)
    tables.scheduler_section.rss = format_bytes(data.scheduler.rss)
    tables.scheduler_section.rss_free = format_bytes(data.rss_free)

    previous_workers = set(tables.workers_section.workers.keys())
    current_workers = set(worker_data.worker_id.decode() for worker_data in data.worker_manager.workers)

    for worker_data in data.worker_manager.workers:
        worker_name = worker_data.worker_id.decode()
        tables.workers_section.workers[worker_name].populate(worker_data)

    for died_worker in previous_workers - current_workers:
        tables.workers_section.workers.pop(died_worker)

    if previous_workers != current_workers:
        tables.workers_section.draw_section.refresh()

    tables.task_stream_section.update_data(tables.workers_section)

    tables.worker_processors.update_data(data.worker_manager.workers)
