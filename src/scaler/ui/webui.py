import dataclasses
import logging
import threading
from functools import partial
from typing import Optional, Tuple

from nicegui import ui

from scaler.config.types.zmq import ZMQConfig
from scaler.io.sync_subscriber import ZMQSyncSubscriber
from scaler.protocol.python.message import StateBalanceAdvice, StateScheduler, StateTask, StateWorker
from scaler.protocol.python.mixins import Message
from scaler.ui.constants import (
    MEMORY_USAGE_UPDATE_INTERVAL,
    TASK_LOG_REFRESH_INTERVAL,
    TASK_STREAM_UPDATE_INTERVAL,
    WORKER_PROCESSORS_REFRESH_INTERVAL,
)
from scaler.ui.live_display import SchedulerSection, WorkersSection
from scaler.ui.memory_window import MemoryChart
from scaler.ui.setting_page import Settings
from scaler.ui.task_graph import TaskStream
from scaler.ui.task_log import TaskLogTable
from scaler.ui.worker_processors import WorkerProcessors
from scaler.utility.formatter import format_bytes, format_percentage
from scaler.utility.logging.utility import setup_logger


@dataclasses.dataclass
class Sections:
    scheduler_section: SchedulerSection
    workers_section: WorkersSection
    task_stream_section: TaskStream
    memory_usage_section: MemoryChart
    tasklog_section: TaskLogTable
    worker_processors: WorkerProcessors
    settings_section: Settings


def start_webui(
    address: str,
    host: str,
    port: int,
    logging_paths: Tuple[str, ...],
    logging_config_file: Optional[str],
    logging_level: str,
):

    setup_logger(logging_paths, logging_config_file, logging_level)

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
            tables.workers_section.draw_section()  # type: ignore[call-arg]

        with ui.tab_panel(tasklog_tab):
            tables.tasklog_section.draw_section()  # type: ignore[call-arg]
            ui.timer(TASK_LOG_REFRESH_INTERVAL, tables.tasklog_section.draw_section.refresh, active=True)

        with ui.tab_panel(stream_tab):
            tables.task_stream_section.setup_task_stream(tables.settings_section)
            ui.timer(TASK_STREAM_UPDATE_INTERVAL, tables.task_stream_section.update_plot, active=True)

            tables.memory_usage_section.setup_memory_chart(tables.settings_section)
            ui.timer(MEMORY_USAGE_UPDATE_INTERVAL, tables.memory_usage_section.update_plot, active=True)

        with ui.tab_panel(worker_processors_tab):
            tables.worker_processors.draw_section()  # type: ignore[call-arg]
            ui.timer(WORKER_PROCESSORS_REFRESH_INTERVAL, tables.worker_processors.draw_section.refresh, active=True)

        with ui.tab_panel(settings_tab):
            tables.settings_section.draw_section()

    subscriber = ZMQSyncSubscriber(
        address=ZMQConfig.from_string(address),
        callback=partial(__show_status, tables=tables),
        topic=b"",
        timeout_seconds=-1,
    )
    subscriber.start()

    ui_thread = threading.Thread(target=partial(ui.run, host=host, port=port, reload=False), daemon=False)
    ui_thread.start()
    ui_thread.join()


def __show_status(status: Message, tables: Sections):
    if isinstance(status, StateScheduler):
        __update_scheduler_state(status, tables)
        return

    if isinstance(status, StateWorker):
        logging.info(f"Received StateWorker update for worker {status.worker_id.decode()} with {status.state.name}")
        tables.scheduler_section.handle_worker_state(status)
        tables.workers_section.handle_worker_state(status)
        tables.task_stream_section.handle_worker_state(status)
        tables.memory_usage_section.handle_worker_state(status)
        tables.tasklog_section.handle_worker_state(status)
        tables.worker_processors.handle_worker_state(status)
        tables.settings_section.handle_worker_state(status)
        return

    if isinstance(status, StateTask):
        logging.debug(f"Received StateTask update for task {status.task_id.hex()} with {status.state.name}")
        tables.scheduler_section.handle_task_state(status)
        tables.workers_section.handle_task_state(status)
        tables.task_stream_section.handle_task_state(status)
        tables.memory_usage_section.handle_task_state(status)
        tables.tasklog_section.handle_task_state(status)
        tables.worker_processors.handle_task_state(status)
        tables.settings_section.handle_task_state(status)
        return

    if isinstance(status, StateBalanceAdvice):
        logging.debug(f"Received StateBalanceAdvice for {status.worker_id.decode()} with {len(status.task_ids)} tasks")
        return

    logging.info(f"Unhandled message received: {type(status)}")


def __update_scheduler_state(data: StateScheduler, tables: Sections):
    tables.scheduler_section.cpu = format_percentage(data.scheduler.cpu)
    tables.scheduler_section.rss = format_bytes(data.scheduler.rss)
    tables.scheduler_section.rss_free = format_bytes(data.rss_free)

    for worker_data in data.worker_manager.workers:
        worker_name = worker_data.worker_id.decode()
        tables.workers_section.workers[worker_name].populate(worker_data)

    tables.worker_processors.update_data(data.worker_manager.workers)
