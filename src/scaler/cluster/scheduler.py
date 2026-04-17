import asyncio
import multiprocessing
import signal
from asyncio import AbstractEventLoop, Task
from typing import Any, Optional, Tuple

from scaler.config.section.scheduler import PolicyConfig, SchedulerConfig
from scaler.config.types.address import AddressConfig
from scaler.scheduler.scheduler import Scheduler
from scaler.utility.event_loop import register_event_loop, run_task_forever
from scaler.utility.logging.utility import setup_logger


class SchedulerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        bind_address: AddressConfig,
        object_storage_address: AddressConfig,
        advertised_object_storage_address: Optional[AddressConfig],
        monitor_address: Optional[AddressConfig],
        io_threads: int,
        max_number_of_tasks_waiting: int,
        client_timeout_seconds: int,
        worker_timeout_seconds: int,
        object_retention_seconds: int,
        load_balance_seconds: int,
        load_balance_trigger_times: int,
        protected: bool,
        policy: PolicyConfig,
        event_loop: str,
        logging_paths: Tuple[str, ...],
        logging_config_file: Optional[str],
        logging_level: str,
    ):
        super().__init__(name="Scheduler")
        self._scheduler_config = SchedulerConfig(
            bind_address=bind_address,
            object_storage_address=object_storage_address,
            advertised_object_storage_address=advertised_object_storage_address,
            monitor_address=monitor_address,
            protected=protected,
            max_number_of_tasks_waiting=max_number_of_tasks_waiting,
            client_timeout_seconds=client_timeout_seconds,
            worker_timeout_seconds=worker_timeout_seconds,
            object_retention_seconds=object_retention_seconds,
            load_balance_seconds=load_balance_seconds,
            load_balance_trigger_times=load_balance_trigger_times,
            event_loop=event_loop,
            io_threads=io_threads,
            policy=policy,
        )

        self._logging_paths = logging_paths
        self._logging_config_file = logging_config_file
        self._logging_level = logging_level

        self._scheduler: Optional[Scheduler] = None
        self._loop: Optional[AbstractEventLoop] = None
        self._task: Optional[Task[Any]] = None

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run())

    async def _run(self) -> None:
        self.__initialize()

        scheduler = Scheduler(self._scheduler_config)
        self._task = self._loop.create_task(scheduler.get_loops())
        self.__register_signal()
        await self._task

    def __initialize(self) -> None:
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        register_event_loop(self._scheduler_config.event_loop)

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__handle_signal)
        self._loop.add_signal_handler(signal.SIGTERM, self.__handle_signal)

    def __handle_signal(self):
        self._loop.call_soon_threadsafe(self._task.cancel)
