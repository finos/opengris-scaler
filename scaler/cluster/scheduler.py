import asyncio
import multiprocessing
import signal
from asyncio import AbstractEventLoop, Task
from typing import Any, Optional

from scaler.config.section.scheduler import SchedulerConfig
from scaler.scheduler.scheduler import Scheduler, scheduler_main
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger


class SchedulerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(self, config: SchedulerConfig):
        multiprocessing.Process.__init__(self, name="Scheduler")
        self._scheduler_config = config

        self._scheduler: Optional[Scheduler] = None
        self._loop: Optional[AbstractEventLoop] = None
        self._task: Optional[Task[Any]] = None

    def run(self) -> None:
        # scheduler have its own single process
        setup_logger(
            self._scheduler_config.logging_paths,
            self._scheduler_config.logging_config_file,
            self._scheduler_config.logging_level,
        )
        register_event_loop(self._scheduler_config.event_loop)

        self._loop = asyncio.get_event_loop()
        SchedulerProcess.__register_signal(self._loop)

        self._task = self._loop.create_task(scheduler_main(self._scheduler_config))

        self._loop.run_until_complete(self._task)

    @staticmethod
    def __register_signal(loop):
        loop.add_signal_handler(signal.SIGINT, SchedulerProcess.__handle_signal)
        loop.add_signal_handler(signal.SIGTERM, SchedulerProcess.__handle_signal)

    @staticmethod
    def __handle_signal():
        for task in asyncio.all_tasks():
            task.cancel()
