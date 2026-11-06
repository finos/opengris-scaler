import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from typing import List, Optional, Set

import psutil

from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

STARTUP_TIMEOUT_SECONDS = 60
SHUTDOWN_TIMEOUT_SECONDS = 30
DESCENDANT_EXIT_TIMEOUT_SECONDS = 15


# the processes bind port 0 and log the ports they got
_BOUND_PORT_LOG_PATTERNS = (
    re.compile(r"ObjectStorageServer: started, listening to tcp://[^:]+:(\d+)"),
    re.compile(r"listen to scheduler address tcp://[^:]+:(\d+)"),
    re.compile(r"listen to scheduler monitor address tcp://[^:]+:(\d+)"),
)


def _bound_ports(log_content: str) -> Set[int]:
    """The ports the object storage, scheduler and monitor logged as bound so far."""
    return {int(match.group(1)) for pattern in _BOUND_PORT_LOG_PATTERNS for match in pattern.finditer(log_content)}


def _ports_listening(ports: Set[int]) -> Set[int]:
    """Return the subset of loopback ports currently accepting TCP connections.

    Probes with an actual connection
    rather than psutil.net_connections(), which requires root privileges on macOS while the
    Python test suite does not run as root."""
    listening = set()
    for port in ports:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=2):
                listening.add(port)
        except OSError:
            pass
    return listening


@unittest.skipIf(sys.platform == "win32", "POSIX signal delivery semantics")
class TestScalerShutdown(unittest.TestCase):
    """The `scaler` CLI must terminate its whole process tree and release all ports when it is
    signalled, including when only the main process receives the signal (e.g. `kill <pid>`) and
    when the signal arrives while the cluster is still starting up."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self._temp_dir = tempfile.TemporaryDirectory()
        self._config_path = os.path.join(self._temp_dir.name, "scaler_all.toml")
        with open(self._config_path, "w") as config_file:
            config_file.write("""
[object_storage_server]
bind_address = "tcp://127.0.0.1:0"

[scheduler]
bind_address = "tcp://127.0.0.1:0"
object_storage_address = "tcp://127.0.0.1:0"
policy_content = "allocate=even_load; scaling=static"
protected = false
event_loop = "builtin"

[[worker_manager]]
type = "baremetal_native"
scheduler_address = "tcp://127.0.0.1:0"
worker_manager_id = "wm-shutdown-test"
object_storage_address = "tcp://127.0.0.1:0"
max_task_concurrency = 1
""")

        self._log_path = os.path.join(self._temp_dir.name, "scaler.log")
        self._process: Optional[subprocess.Popen] = None

    def tearDown(self) -> None:
        if self._process is not None:
            self._kill_process_tree(self._process)
        self._temp_dir.cleanup()

    def _start_scaler(self) -> subprocess.Popen:
        with open(self._log_path, "wb") as log_file:
            self._process = subprocess.Popen(
                [sys.executable, "-m", "scaler.entry_points.scaler", self._config_path],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,  # own process group: signals to the main process only
            )
        return self._process

    def _kill_process_tree(self, process: subprocess.Popen) -> None:
        try:
            descendants = psutil.Process(process.pid).children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            descendants = []
        # The platform check is for mypy on Windows; the whole test class is POSIX-only.
        if sys.platform != "win32":
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        for descendant in descendants:
            try:
                descendant.kill()
            except psutil.NoSuchProcess:
                pass
        process.wait(timeout=10)

    def _wait_for_startup(self, process: subprocess.Popen) -> None:
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if process.poll() is not None:
                with open(self._log_path) as log_file:
                    self.fail(f"scaler exited during startup (code {process.returncode}):\n{log_file.read()}")
            ports = _bound_ports(self._read_log())
            if len(ports) == len(_BOUND_PORT_LOG_PATTERNS) and _ports_listening(ports) == ports:
                return
            time.sleep(0.25)
        self.fail(f"cluster did not start within {STARTUP_TIMEOUT_SECONDS}s:\n{self._read_log()}")

    def _read_log(self) -> str:
        with open(self._log_path) as log_file:
            return log_file.read()

    def _descendants(self, process: subprocess.Popen) -> List[psutil.Process]:
        try:
            return psutil.Process(process.pid).children(recursive=True)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return []

    def _assert_clean_exit(self, process: subprocess.Popen, descendants: List[psutil.Process]) -> None:
        try:
            process.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
        except subprocess.TimeoutExpired:
            with open(self._log_path) as log_file:
                log_content = log_file.read()
            self.fail(f"scaler main process did not exit within {SHUTDOWN_TIMEOUT_SECONDS}s:\n{log_content}")

        _, alive = psutil.wait_procs(descendants, timeout=DESCENDANT_EXIT_TIMEOUT_SECONDS)
        survivors = []
        for survivor in alive:
            try:
                survivors.append(" ".join(survivor.cmdline()))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                survivors.append(f"pid={survivor.pid}")
        self.assertEqual([], survivors, "descendant processes survived shutdown")

        ports = _bound_ports(self._read_log())
        deadline = time.monotonic() + DESCENDANT_EXIT_TIMEOUT_SECONDS
        while time.monotonic() < deadline and _ports_listening(ports):
            time.sleep(0.25)
        self.assertEqual(set(), _ports_listening(ports), "ports still bound after shutdown")

    def test_sigterm_terminates_whole_cluster(self) -> None:
        process = self._start_scaler()
        self._wait_for_startup(process)
        descendants = self._descendants(process)
        self.assertGreater(len(descendants), 0)

        process.send_signal(signal.SIGTERM)
        self._assert_clean_exit(process, descendants)

    def test_sigint_terminates_whole_cluster(self) -> None:
        process = self._start_scaler()
        self._wait_for_startup(process)
        descendants = self._descendants(process)
        self.assertGreater(len(descendants), 0)

        process.send_signal(signal.SIGINT)
        self._assert_clean_exit(process, descendants)

    def test_sigint_during_startup_does_not_hang(self) -> None:
        # sweep the delay: what decides the outcome is which YMQ callback is in flight when the interrupt
        # unwinds, and a single delay reproduces the deadlock most of the time but not every time
        for delay_seconds in (0.1, 0.3, 0.5, 0.7, 0.9):
            with self.subTest(delay_seconds=delay_seconds):
                self.setUp()
                try:
                    process = self._start_scaler()
                    time.sleep(delay_seconds)
                    descendants = self._descendants(process)

                    if process.poll() is None:
                        process.send_signal(signal.SIGINT)
                    self._assert_clean_exit(process, descendants)
                finally:
                    self.tearDown()
