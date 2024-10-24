import asyncio
import multiprocessing
from asyncio import AbstractEventLoop, Task
from typing import Any, Optional, Tuple

from scaler.scheduler.config import SchedulerConfig
from scaler.scheduler.scheduler import Scheduler, scheduler_main
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.utility.zmq_config import ZMQConfig


class SchedulerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        address: ZMQConfig,
        io_threads: int,
        max_number_of_tasks_waiting: int,
        per_worker_queue_size: int,
        client_timeout_seconds: int,
        worker_timeout_seconds: int,
        object_retention_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
        protected: bool,
        event_loop: str,
        logging_path: Tuple[str, ...],
        logging_config_file: Optional[str],
    ):
        multiprocessing.Process.__init__(self, name="Scheduler")
        self._scheduler_config = SchedulerConfig(
            event_loop=event_loop,
            address=address,
            io_threads=io_threads,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            per_worker_queue_size=per_worker_queue_size,
            client_timeout_seconds=client_timeout_seconds,
            worker_timeout_seconds=worker_timeout_seconds,
            object_retention_seconds=object_retention_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
            protected=protected,
        )

        self._logging_path = logging_path
        self._logging_config_file = logging_config_file

        self._scheduler: Optional[Scheduler] = None
        self._loop: Optional[AbstractEventLoop] = None
        self._task: Optional[Task[Any]] = None

    def run(self) -> None:
        # scheduler have its own single process
        setup_logger(self._logging_path, self._logging_config_file)
        register_event_loop(self._scheduler_config.event_loop)

        self._loop = asyncio.get_event_loop()
        self._task = self._loop.create_task(scheduler_main(self._scheduler_config))

        self._loop.run_until_complete(self._task)
