# PYTHON_ARGCOMPLETE_OK
from typing import Callable, Optional

from scaler.cluster.scheduler import run_scheduler
from scaler.config.section.scheduler import SchedulerConfig
from scaler.scheduler.scheduler import BoundSchedulerAddresses


def main(
    scheduler_config: Optional[SchedulerConfig] = None,
    on_ready: Optional[Callable[[BoundSchedulerAddresses], None]] = None,
) -> None:
    if scheduler_config is None:
        scheduler_config = SchedulerConfig.parse("Scaler Scheduler", "scheduler")

    # Run the scheduler in this process rather than in a child process: a child would be
    # orphaned (and keep the scheduler ports bound) whenever this process is terminated
    # by a signal, since nothing would forward the signal to it.
    run_scheduler(
        scheduler_config,
        scheduler_config.logging_config.paths,
        scheduler_config.logging_config.config_file,
        scheduler_config.logging_config.level,
        on_ready=on_ready,
    )
