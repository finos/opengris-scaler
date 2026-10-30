import asyncio
import multiprocessing
import multiprocessing.connection
from multiprocessing.synchronize import Event as EventType
from typing import Callable, Optional

from scaler.config.common.security import SecurityConfig
from scaler.config.constants import SCHEDULER_READINESS_TIMEOUT_SECONDS
from scaler.config.section.scheduler import PolicyConfig, SchedulerConfig
from scaler.config.types.address import AddressConfig
from scaler.scheduler.scheduler import Scheduler
from scaler.utility.event_loop import register_event_loop, run_task_forever
from scaler.utility.process_bootstrap import bootstrap_process
from scaler.utility.signal_handler import install_async_shutdown_handler


def run_scheduler(
    scheduler_config: SchedulerConfig,
    logging_paths: tuple[str, ...],
    logging_config_file: Optional[str],
    logging_level: str,
    shutdown_event: Optional[EventType] = None,
    on_ready: Optional[Callable[[], None]] = None,
) -> None:
    """Run the scheduler in the current process until SIGINT/SIGTERM (or the optional
    `shutdown_event`) triggers a graceful shutdown. Blocks until the scheduler exited.
    Calls the optional `on_ready` once the scheduler listens."""

    loop = asyncio.new_event_loop()

    async def _run() -> None:
        bootstrap_process(logging_paths, logging_config_file, logging_level)
        register_event_loop(scheduler_config.event_loop)

        scheduler = Scheduler(scheduler_config)
        task = loop.create_task(scheduler.get_loops(on_ready))

        def _cancel_scheduler_task() -> None:
            loop.call_soon_threadsafe(task.cancel)

        install_async_shutdown_handler(loop, _cancel_scheduler_task, shutdown_event)
        await task

    run_task_forever(loop, _run())


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
        logging_paths: tuple[str, ...],
        logging_config_file: Optional[str],
        logging_level: str,
        shutdown_event: Optional[EventType] = None,
        security_config: Optional[SecurityConfig] = None,
    ):
        super().__init__(name="Scheduler")

        security_config = security_config if security_config is not None else SecurityConfig()

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
            security=security_config,
        )

        self._logging_paths = logging_paths
        self._logging_config_file = logging_config_file
        self._logging_level = logging_level

        self._shutdown_event = shutdown_event
        self._ready_receiver, self._ready_sender = multiprocessing.get_context("spawn").Pipe(duplex=False)

    def wait_until_ready(self) -> None:
        """Blocks until the scheduler listens, so a client connecting next does not race its startup."""
        address = self._scheduler_config.bind_address
        # the sentinel wakes the wait if the scheduler exits first, without polling its exit code
        ready = multiprocessing.connection.wait(
            [self._ready_receiver, self.sentinel], SCHEDULER_READINESS_TIMEOUT_SECONDS
        )
        if self._ready_receiver in ready:
            return

        if self.sentinel in ready:
            self.join()
            raise RuntimeError(f"Scheduler exited with code {self.exitcode} before listening to {address!r}")

        raise TimeoutError(
            f"Scheduler at {address!r} failed to start within {SCHEDULER_READINESS_TIMEOUT_SECONDS} seconds"
        )

    def run(self) -> None:
        run_scheduler(
            self._scheduler_config,
            self._logging_paths,
            self._logging_config_file,
            self._logging_level,
            self._shutdown_event,
            lambda: self._ready_sender.send(True),
        )
