"""Shared harness for the end-to-end integration skeleton.

Brings up a real object-storage server + scheduler (in their own processes, exactly like
production) with clean, exception-safe teardown, plus small polling helpers. Kept separate
from the AWS backend so the same harness serves both the cloud-mock control-plane tests and
the local data-plane tests.
"""

from __future__ import annotations

import asyncio
import time
from typing import Callable

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.utility.network_util import get_available_tcp_port

DEFAULT_WAIT_TIMEOUT_SECONDS = 30.0
_POLL_INTERVAL_SECONDS = 0.1


def wait_until(predicate: Callable[[], bool], timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS, message: str = "") -> None:
    """Block until ``predicate()`` is truthy or ``timeout`` elapses (raises TimeoutError)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(_POLL_INTERVAL_SECONDS)
    raise TimeoutError(message or f"condition not met within {timeout:.1f}s")


async def async_wait_until(
    predicate: Callable[[], bool], timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS, message: str = ""
) -> None:
    """Async variant of :func:`wait_until`, yielding to the loop between polls."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(_POLL_INTERVAL_SECONDS)
    raise TimeoutError(message or f"condition not met within {timeout:.1f}s")


class SchedulerHarness:
    """A running object-storage server + scheduler, addressed over loopback TCP.

    Unlike ``SchedulerClusterCombo`` this does NOT start any workers of its own, so a test
    can attach its own (dynamic / cloud) worker manager and observe the full provisioning
    control loop. Register :meth:`shutdown` with ``self.addCleanup`` so it is torn down even
    if the test body raises.
    """

    def __init__(self, scaling_policy: str = "allocate=even_load; scaling=vanilla") -> None:
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
        self._object_storage.wait_until_ready()

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
        self._shutdown_done = False

    @property
    def object_storage_address(self) -> AddressConfig:
        return self._object_storage_address

    def shutdown(self) -> None:
        if self._shutdown_done:
            return
        self._shutdown_done = True
        import os
        import signal

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
            process.join(timeout=15)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
