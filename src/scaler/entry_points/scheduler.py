# PYTHON_ARGCOMPLETE_OK
from typing import Optional

from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.section.scheduler import SchedulerConfig


def main(scheduler_config: Optional[SchedulerConfig] = None) -> None:
    if scheduler_config is None:
        scheduler_config = SchedulerConfig.parse("Scaler Scheduler", "scheduler")

    scheduler = SchedulerProcess(
        bind_address=scheduler_config.bind_address,
        object_storage_address=scheduler_config.object_storage_address,
        monitor_address=scheduler_config.monitor_address,
        io_threads=scheduler_config.io_threads,
        max_number_of_tasks_waiting=scheduler_config.max_number_of_tasks_waiting,
        client_timeout_seconds=scheduler_config.client_timeout_seconds,
        worker_timeout_seconds=scheduler_config.worker_timeout_seconds,
        object_retention_seconds=scheduler_config.object_retention_seconds,
        load_balance_seconds=scheduler_config.load_balance_seconds,
        load_balance_trigger_times=scheduler_config.load_balance_trigger_times,
        protected=scheduler_config.protected,
        event_loop=scheduler_config.event_loop,
        policy=scheduler_config.policy,
        logging_paths=scheduler_config.logging_config.paths,
        logging_config_file=scheduler_config.logging_config.config_file,
        logging_level=scheduler_config.logging_config.level,
    )
    scheduler.start()

    scheduler.join()
