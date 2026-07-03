import logging
import multiprocessing
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_PATHS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager
from tests.utility.utility import logging_test_name

# Deliberately short worker death-timeout (vs the 5-min DEFAULT_WORKER_DEATH_TIMEOUT) so a worker
# that never reaches a scheduler self-terminates quickly and keeps the test fast.
DEATH_TIMEOUT_SECONDS = 10
# Upper bound on cluster teardown time.
TEARDOWN_TIMEOUT_SECONDS = 30
# A no-scheduler worker manager exits only after its workers exhaust their connection-retry backoff
# (~50s measured), so bound the wait well above that: a real regression fails instead of hanging CI.
NO_SCHEDULER_EXIT_TIMEOUT_SECONDS = 120
# Poll interval for the "process has actually exited" waits below.
POLL_INTERVAL_SECONDS = 0.05


class TestDeathTimeout(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    @staticmethod
    def _terminate_process(process: multiprocessing.process.BaseProcess) -> None:
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join()

    def test_no_scheduler(self):
        logging.info("test with no scheduler")
        # Test 1: Spinning up a cluster with no scheduler. Death timeout should apply
        manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"),
                    worker_manager_id="test_manager",
                    object_storage_address=None,
                    max_task_concurrency=2,
                ),
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=DEFAULT_PER_WORKER_QUEUE_SIZE,
                    heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                    garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                    trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                    task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                    death_timeout_seconds=DEATH_TIMEOUT_SECONDS,
                    hard_processor_suspend=False,
                    io_threads=DEFAULT_IO_THREADS,
                    event_loop="builtin",
                ),
                logging_config=LoggingConfig(
                    paths=DEFAULT_LOGGING_PATHS, level=DEFAULT_LOGGING_LEVEL, config_file=None
                ),
            )
        )
        process = multiprocessing.get_context("spawn").Process(target=manager.run)
        process.start()
        self.addCleanup(self._terminate_process, process)

        # With no scheduler to reach, the workers must self-terminate once they exhaust their connection
        # retries, which lets run() return and the process exit cleanly. Bound the join so a regression
        # that breaks this fails the test instead of hanging CI forever.
        process.join(timeout=NO_SCHEDULER_EXIT_TIMEOUT_SECONDS)
        self.assertFalse(process.is_alive(), "worker manager did not exit after losing its scheduler")
        self.assertEqual(process.exitcode, 0)

    def test_shutdown(self):
        logging.info("test with explicitly shutdown")
        # Test 2: Running the Combo and sending shutdown
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address, n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=False
        )
        # Unprotected combo: client.shutdown() stops the workers and the scheduler, but the object
        # storage server still needs cluster.shutdown() -- via cleanup so it runs even if an assert raises.
        self.addCleanup(cluster.shutdown)

        with Client(address=address) as client:
            # Run a real task first: a completed result proves the workers have connected and the cluster is
            # fully up, which replaces the fixed warm-up sleep with an actual readiness condition.
            self.assertEqual(client.submit(round, 3.14).result(), 3)

            logging.info("Shutting down")
            client.shutdown()

        # Poll until the worker manager process has actually exited instead of sleeping a fixed amount.
        deadline = time.monotonic() + TEARDOWN_TIMEOUT_SECONDS
        while cluster._worker_manager_process.is_alive() and time.monotonic() < deadline:
            time.sleep(POLL_INTERVAL_SECONDS)
        self.assertFalse(
            cluster._worker_manager_process.is_alive(), "client.shutdown() did not stop the worker cluster"
        )

    def test_no_timeout_if_suspended(self):
        """
        Client and scheduler shouldn't time out a client if it is running inside a suspended processor.
        """

        client_timeout_seconds = 3

        def parent(c: Client):
            return c.submit(child).result()

        def child():
            time.sleep(client_timeout_seconds + 1)  # prevents the parent task to execute.
            return "OK"

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address,
            n_workers=1,
            per_worker_task_queue_size=2,
            event_loop="builtin",
            client_timeout_seconds=client_timeout_seconds,
        )

        try:
            with Client(address, timeout_seconds=client_timeout_seconds) as client:
                future = client.submit(parent, client)
                self.assertEqual(future.result(), "OK")
        finally:
            cluster.shutdown()
