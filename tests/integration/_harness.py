"""Shared harness for the container-scaling e2e and the AWS control-plane integration tests.

Brings up a real object-storage server + scheduler (in their own processes, exactly like production)
with clean, exception-safe teardown, plus the small polling helper and the scheduler-command helper.
Optionally starts the web GUI wired to the scheduler's monitor address so a local run can be watched in
a browser. Kept separate from the AWS backend so the same harness serves both the AWS control-plane
tests and the container-scaling data-plane tests.
"""

from __future__ import annotations

import asyncio
import os
import signal
import time
from multiprocessing import get_context
from multiprocessing.process import BaseProcess
from typing import Callable, Optional

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
from scaler.scheduler.controllers.worker_manager_utilties import build_set_desired_command
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import POLL_INTERVAL_SECONDS, PROCESS_TERMINATION_TIMEOUT_SECONDS

DEFAULT_WAIT_TIMEOUT_SECONDS = 30.0
# get_available_tcp_port() releases the port before the child binds, so a since-freed ephemeral port
# can be grabbed in between (a TOCTOU race). Retry the whole bring-up with fresh ports a few times.
_HARNESS_START_ATTEMPTS = 3


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


def _run_webgui(monitor_address: str, host: str, port: int) -> None:
    """Process entry point for the web GUI (blocking uvicorn server); it subscribes to the scheduler's
    monitor address and serves the dashboard on ``host:port``."""
    from scaler.config.section.webgui import WebGUIConfig
    from scaler.config.types.address import AddressConfig as _AddressConfig
    from scaler.config.types.http import HTTPConfig
    from scaler.ui.webgui import start_webgui

    start_webgui(
        WebGUIConfig(monitor_address=_AddressConfig.from_string(monitor_address), gui_address=HTTPConfig(host, port))
    )


class SchedulerHarness:
    """A running object-storage server + scheduler, addressed over loopback TCP.

    Unlike ``SchedulerClusterCombo`` this does NOT start any workers of its own, so a test can attach
    its own (dynamic / cloud) worker manager and observe the full provisioning control loop. Register
    :meth:`shutdown` with ``self.addCleanup`` so it is torn down even if the test body raises.
    """

    def __init__(
        self,
        policy_content: str = "allocate=even_load; scaling=vanilla",
        policy_engine_type: str = "simple",
        gateway: Optional[str] = None,
        enable_webgui: bool = False,
    ) -> None:
        # gateway: a docker-bridge gateway IP. When set, the scheduler + object storage bind 0.0.0.0 and
        # advertise gateway-reachable addresses so workers running in containers can connect back; the
        # host client still uses loopback. None keeps the original loopback-only behavior.
        self._gateway = gateway
        self._enable_webgui = enable_webgui
        self._object_storage: Optional[ObjectStorageServerProcess] = None
        self._scheduler: Optional[SchedulerProcess] = None
        self._webgui: Optional[BaseProcess] = None
        self._monitor_client_address: Optional[str] = None
        self._shutdown_done = False

        errors: list = []
        for _attempt in range(_HARNESS_START_ATTEMPTS):
            try:
                self._bring_up(policy_content, policy_engine_type)
                break
            except (OSError, TimeoutError) as error:
                # A since-freed ephemeral port was grabbed before a child could bind it; tear the
                # partial bring-up down and retry with fresh ports.
                errors.append(error)
                self._kill_processes()
        else:
            raise RuntimeError(
                f"SchedulerHarness failed to start after {_HARNESS_START_ATTEMPTS} attempts: {errors!r}"
            ) from errors[-1]

        if self._enable_webgui:
            self._start_webgui()

    def _bring_up(self, policy_content: str, policy_engine_type: str) -> None:
        sched_port, os_port = get_available_tcp_port(), get_available_tcp_port()
        bind_host = "0.0.0.0" if self._gateway else "127.0.0.1"
        # The host client always connects over loopback; container workers use the gateway address.
        self.scheduler_address = f"tcp://127.0.0.1:{sched_port}"
        self.worker_scheduler_address = (
            f"tcp://{self._gateway}:{sched_port}" if self._gateway else self.scheduler_address
        )
        self._object_storage_address = AddressConfig.from_string(f"tcp://{bind_host}:{os_port}")
        advertised_os = AddressConfig.from_string(f"tcp://{self._gateway}:{os_port}") if self._gateway else None

        monitor_address = None
        if self._enable_webgui:
            monitor_port = get_available_tcp_port()
            monitor_address = AddressConfig.from_string(f"tcp://{bind_host}:{monitor_port}")
            self._monitor_client_address = f"tcp://127.0.0.1:{monitor_port}"

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
            bind_address=AddressConfig.from_string(f"tcp://{bind_host}:{sched_port}"),
            object_storage_address=AddressConfig.from_string(f"tcp://127.0.0.1:{os_port}"),
            advertised_object_storage_address=advertised_os,
            monitor_address=monitor_address,
            policy=PolicyConfig(policy_engine_type=policy_engine_type, policy_content=policy_content),
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

    def _start_webgui(self) -> None:
        """Start the dashboard, wired to the scheduler monitor, and print its URL. Best-effort: a missing
        ``[gui]`` extra must not fail the e2e (the GUI is a convenience, not the thing under test)."""
        try:
            import scaler.ui.webgui  # noqa: F401
        except Exception as error:
            print(f"[harness] web GUI unavailable ({error}); continuing without it")
            return
        port = get_available_tcp_port()
        self._webgui = get_context("spawn").Process(
            target=_run_webgui, args=(self._monitor_client_address, "0.0.0.0", port), daemon=True
        )
        self._webgui.start()
        print(f"[harness] web GUI: http://localhost:{port}  (scheduler monitor {self._monitor_client_address})")

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
        if self._webgui is not None and self._webgui.is_alive():
            self._webgui.terminate()
            self._webgui.join(timeout=PROCESS_TERMINATION_TIMEOUT_SECONDS)
            if self._webgui.is_alive():
                self._webgui.kill()
                self._webgui.join()
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
