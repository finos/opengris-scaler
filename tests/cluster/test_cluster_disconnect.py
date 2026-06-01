import multiprocessing
import os
import signal
import sys
import time
import unittest
from concurrent.futures import CancelledError, TimeoutError

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager
from tests.utility.utility import logging_test_name

_GRACEFUL_SHUTDOWN_MAX_SECONDS = 10
_HEARTBEAT_TIMEOUT_SECONDS = 120


def noop_sleep(sec: int):
    time.sleep(sec)
    return sec


class TestClusterDisconnect(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(n_workers=0, event_loop="builtin")
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()
        pass

    def test_cluster_disconnect(self):
        base_config = self.combo._worker_manager.config
        base_worker_config = base_config.worker_config
        base_logging_config = base_config.logging_config
        dying_manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=self.combo._address,
                    worker_manager_id="test_manager",
                    object_storage_address=self.combo._object_storage_address,
                    max_task_concurrency=1,
                ),
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=base_worker_config.per_worker_task_queue_size,
                    heartbeat_interval_seconds=base_worker_config.heartbeat_interval_seconds,
                    task_timeout_seconds=base_worker_config.task_timeout_seconds,
                    death_timeout_seconds=base_worker_config.death_timeout_seconds,
                    garbage_collect_interval_seconds=base_worker_config.garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=base_worker_config.trim_memory_threshold_bytes,
                    hard_processor_suspend=base_worker_config.hard_processor_suspend,
                    io_threads=base_worker_config.io_threads,
                    event_loop=base_worker_config.event_loop,
                ),
                logging_config=LoggingConfig(
                    paths=DEFAULT_LOGGING_PATHS,
                    level=base_logging_config.level,
                    config_file=base_logging_config.config_file,
                ),
            )
        )
        dying_process = multiprocessing.get_context("spawn").Process(target=dying_manager.run)
        dying_process.start()

        client = Client(self.address)
        future_result = client.submit(noop_sleep, 5)
        time.sleep(2)
        dying_process.terminate()
        dying_process.join()

        with self.assertRaises(CancelledError):
            client.clear()
            future_result.result()


@unittest.skipIf(
    sys.platform == "win32",
    "os.kill(pid, SIGINT) cannot deliver SIGINT to a spawn child on Windows; "
    "graceful shutdown via SIGINT is POSIX-only.",
)
class TestGracefulWorkerShutdown(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(
            n_workers=0,
            event_loop="builtin",
            worker_timeout_seconds=_HEARTBEAT_TIMEOUT_SECONDS,
        )
        self.address = self.combo.get_address()
        self.client = Client(self.address)
        self._manager_processes: list = []

    def tearDown(self) -> None:
        for proc in self._manager_processes:
            if proc.is_alive():
                proc.terminate()
                proc.join()
        self.client.disconnect()
        self.combo.shutdown()

    def _start_manager(self, manager_id: str) -> multiprocessing.Process:
        base_config = self.combo._worker_manager.config
        base_worker_config = base_config.worker_config
        base_logging_config = base_config.logging_config
        manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=self.combo._address,
                    worker_manager_id=manager_id,
                    object_storage_address=self.combo._object_storage_address,
                    max_task_concurrency=1,
                ),
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=base_worker_config.per_worker_task_queue_size,
                    heartbeat_interval_seconds=base_worker_config.heartbeat_interval_seconds,
                    task_timeout_seconds=base_worker_config.task_timeout_seconds,
                    death_timeout_seconds=base_worker_config.death_timeout_seconds,
                    garbage_collect_interval_seconds=base_worker_config.garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=base_worker_config.trim_memory_threshold_bytes,
                    hard_processor_suspend=base_worker_config.hard_processor_suspend,
                    io_threads=base_worker_config.io_threads,
                    event_loop=base_worker_config.event_loop,
                ),
                logging_config=LoggingConfig(
                    paths=DEFAULT_LOGGING_PATHS,
                    level=base_logging_config.level,
                    config_file=base_logging_config.config_file,
                ),
            )
        )
        proc = multiprocessing.get_context("spawn").Process(target=manager.run)
        proc.start()
        self._manager_processes.append(proc)
        return proc

    def test_graceful_shutdown_re_dispatches_task_immediately(self) -> None:
        proc_a = self._start_manager("manager_a")
        future = self.client.submit(noop_sleep, 5)
        time.sleep(2)  # Let task reach worker A

        # Graceful SIGINT: manager sends SIGTERM to worker → worker sends WDN → scheduler
        # re-queues the task immediately. Without WDN the task would stay assigned to the
        # dead worker until the heartbeat timeout (_HEARTBEAT_TIMEOUT_SECONDS).
        os.kill(proc_a.pid, signal.SIGINT)

        self._start_manager("manager_b")  # worker B picks up the re-queued task

        try:
            result = future.result(timeout=_GRACEFUL_SHUTDOWN_MAX_SECONDS)
            self.assertEqual(result, 5)
        except TimeoutError:
            self.fail(
                f"Task was not re-dispatched within {_GRACEFUL_SHUTDOWN_MAX_SECONDS}s — "
                "WorkerDisconnectNotification may not have reached the scheduler "
                f"(heartbeat timeout is {_HEARTBEAT_TIMEOUT_SECONDS}s)"
            )
