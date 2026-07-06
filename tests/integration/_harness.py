"""Shared harness for the end-to-end integration skeleton.

Brings up a real object-storage server + scheduler (in their own processes, exactly like production)
with clean, exception-safe teardown, plus the small polling helper and the shared native-worker-manager
entry point. Kept separate from the AWS backend so the same harness serves both the cloud-mock
control-plane tests and the local data-plane tests.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from typing import Callable, Optional

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.scheduler.controllers.worker_manager_utilties import build_set_desired_command
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import POLL_INTERVAL_SECONDS, PROCESS_TERMINATION_TIMEOUT_SECONDS

DEFAULT_WAIT_TIMEOUT_SECONDS = 30.0
# get_available_tcp_port() releases the port before the child binds, so a since-freed ephemeral port
# can be grabbed in between (a TOCTOU race). Retry the whole bring-up with fresh ports a few times.
_HARNESS_START_ATTEMPTS = 3
_PER_WORKER_TASK_QUEUE_SIZE = 10


async def async_wait_until(
    predicate: Callable[[], bool], timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS, message: str = ""
) -> None:
    """Poll ``predicate``, yielding to the event loop between checks, until it is true or ``timeout``
    elapses (then raise ``TimeoutError``)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(message or f"condition not met within {timeout:.1f}s")


def desired_requests(count: int) -> list:
    """The exact per-capset request list the scheduler's scaling policy sends over the wire for a target
    concurrency (shared by the ECS and EC2/ORB control-plane tests)."""
    return list(build_set_desired_command([({}, count)]).setDesiredTaskConcurrencyRequests)


def run_native_worker_manager(
    scheduler_address: str,
    max_task_concurrency: int,
    worker_manager_id: str = "wm-native-it",
    machine_id: Optional[str] = None,
) -> None:
    """Process entry point for a dynamic (auto-scaling) native worker manager, shared by the local
    data-plane and scaling-stress tests. When ``machine_id`` is set it is exported as
    ``SCALER_IT_MACHINE_ID`` *before* the manager spawns workers so the worker/processor procs inherit
    it, letting a task report which manager provisioned the worker that ran it."""
    from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

    if machine_id is not None:
        os.environ["SCALER_IT_MACHINE_ID"] = machine_id

    manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_manager_id=worker_manager_id,
                object_storage_address=None,
                max_task_concurrency=max_task_concurrency,
            ),
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=_PER_WORKER_TASK_QUEUE_SIZE,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
                io_threads=DEFAULT_IO_THREADS,
                event_loop="builtin",
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )
    manager.run()


class SchedulerHarness:
    """A running object-storage server + scheduler, addressed over loopback TCP.

    Unlike ``SchedulerClusterCombo`` this does NOT start any workers of its own, so a test can attach
    its own (dynamic / cloud) worker manager and observe the full provisioning control loop. Register
    :meth:`shutdown` with ``self.addCleanup`` so it is torn down even if the test body raises.
    """

    def __init__(self, scaling_policy: str = "allocate=even_load; scaling=vanilla") -> None:
        self._object_storage: Optional[ObjectStorageServerProcess] = None
        self._scheduler: Optional[SchedulerProcess] = None
        self._shutdown_done = False

        errors: list = []
        for _attempt in range(_HARNESS_START_ATTEMPTS):
            try:
                self._bring_up(scaling_policy)
                return
            except (OSError, TimeoutError) as error:
                # A since-freed ephemeral port was grabbed before a child could bind it; tear the
                # partial bring-up down and retry with fresh ports.
                errors.append(error)
                self._kill_processes()
        raise RuntimeError(
            f"SchedulerHarness failed to start after {_HARNESS_START_ATTEMPTS} attempts: {errors!r}"
        ) from errors[-1]

    def _bring_up(self, scaling_policy: str) -> None:
        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._object_storage_address = AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}")

        self._object_storage = ObjectStorageServerProcess(
            bind_address=self._object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        self._object_storage.start()
        self._object_storage.wait_until_ready()  # raises TimeoutError if the port was taken

        self._scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(self.scheduler_address),
            object_storage_address=self._object_storage_address,
            advertised_object_storage_address=None,
            monitor_address=None,
            policy=PolicyConfig(policy_content=scaling_policy),
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        self._scheduler.start()

    @property
    def object_storage_address(self) -> AddressConfig:
        return self._object_storage_address

    def _kill_processes(self) -> None:
        """Force-terminate whatever is running (used to clean up a partial bring-up between retries)."""
        for process in (self._scheduler, self._object_storage):
            if process is not None and process.is_alive():
                process.terminate()
                process.join(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
                if process.is_alive():
                    process.kill()
                    process.join()
        self._scheduler = None
        self._object_storage = None

    def shutdown(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        for process, use_sigint in ((self._scheduler, True), (self._object_storage, False)):
            if process is None or not process.is_alive():
                continue
            try:
                if use_sigint:
                    os.kill(process.pid, signal.SIGINT)
                else:
                    process.terminate()
            except (ProcessLookupError, OSError):
                pass
            process.join(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
            if process.is_alive():
                process.kill()
                process.join()
