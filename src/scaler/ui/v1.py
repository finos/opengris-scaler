import threading
from functools import partial

from nicegui import ui

from scaler.config.types.zmq import ZMQConfig
from scaler.io.sync_subscriber import ZMQSyncSubscriber
from scaler.ui.common.constants import (
    MEMORY_USAGE_UPDATE_INTERVAL,
    TASK_LOG_REFRESH_INTERVAL,
    TASK_STREAM_UPDATE_INTERVAL,
    WORKER_PROCESSORS_REFRESH_INTERVAL,
)
from scaler.ui.common.live_display import SchedulerSection, WorkersSection
from scaler.ui.common.memory_window import MemoryChart
from scaler.ui.common.setting_page import Settings
from scaler.ui.common.task_graph import TaskStream
from scaler.ui.common.task_log import TaskLogTable
from scaler.ui.common.webui import Sections, process_scheduler_message
from scaler.ui.common.worker_processors import WorkerProcessors


def start_webui_v1(address: str, host: str, port: int):
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
        callback=partial(process_scheduler_message, tables=tables),
        topic=b"",
        timeout_seconds=-1,
    )
    subscriber.start()

    ui_thread = threading.Thread(target=partial(ui.run, host=host, port=port, reload=False), daemon=False)
    ui_thread.start()
    ui_thread.join()
