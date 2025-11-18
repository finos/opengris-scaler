import dataclasses
import logging

from scaler.protocol.python.message import StateBalanceAdvice, StateScheduler, StateTask, StateWorker
from scaler.protocol.python.mixins import Message
from scaler.ui.common.live_display import SchedulerSection, WorkersSection
from scaler.ui.common.memory_window import MemoryChart
from scaler.ui.common.setting_page import Settings
from scaler.ui.common.task_graph import TaskStream
from scaler.ui.common.task_log import TaskLogTable
from scaler.ui.common.worker_processors import WorkerProcessors
from scaler.utility.formatter import format_bytes, format_percentage


@dataclasses.dataclass
class Sections:
    scheduler_section: SchedulerSection
    workers_section: WorkersSection
    task_stream_section: TaskStream
    memory_usage_section: MemoryChart
    tasklog_section: TaskLogTable
    worker_processors: WorkerProcessors
    settings_section: Settings


def process_scheduler_message(status: Message, tables: Sections):
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

    previous_workers = set(tables.workers_section.workers.keys())
    current_workers = set(worker_data.worker_id.decode() for worker_data in data.worker_manager.workers)

    for worker_data in data.worker_manager.workers:
        worker_name = worker_data.worker_id.decode()
        tables.workers_section.workers[worker_name].populate(worker_data)

    for died_worker in previous_workers - current_workers:
        tables.workers_section.workers.pop(died_worker)
        tables.worker_processors.remove_worker(died_worker)
        tables.task_stream_section.mark_dead_worker(died_worker)

    if previous_workers != current_workers:
        tables.workers_section.draw_section.refresh()

    tables.worker_processors.update_data(data.worker_manager.workers)
