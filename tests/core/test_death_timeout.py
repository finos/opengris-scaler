import logging
import socket
import time
import unittest

from scaler import Client, SchedulerClusterCombo
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
from scaler.config.types.address import AddressConfig
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import OS_PICKED_PORT
from scaler.worker.worker import Worker
from tests.utility.utility import logging_test_name

# This is a manual test because it can loop infinitely if it fails


class TestDeathTimeout(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_no_scheduler(self):
        """A worker that never reaches a scheduler quits once its death timeout passes."""
        # a bound socket that never listens refuses every connection, and no other process can take its port
        refusing_socket = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self.addCleanup(refusing_socket.close)
        refusing_socket.bind(("127.0.0.1", OS_PICKED_PORT))
        refusing_port = refusing_socket.getsockname()[1]

        worker = Worker(
            event_loop="builtin",
            name="no_scheduler",
            address=AddressConfig.from_string(f"tcp://127.0.0.1:{refusing_port}"),
            object_storage_address=None,
            preload=None,
            capabilities={},
            io_threads=DEFAULT_IO_THREADS,
            task_queue_size=DEFAULT_PER_WORKER_QUEUE_SIZE,
            heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
            garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
            trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
            task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
            death_timeout_seconds=10,
            hard_processor_suspend=False,
            logging_paths=DEFAULT_LOGGING_PATHS,
            logging_level=DEFAULT_LOGGING_LEVEL,
            worker_manager_id=b"test_manager",
        )
        worker.start()
        worker.join()

    def test_shutdown(self):
        logging.info("test with explicitly shutdown")
        # Test 2: Running the Combo and sending shutdown
        cluster = SchedulerClusterCombo(
            n_workers=2, per_worker_task_queue_size=2, event_loop="builtin", protected=False
        )
        address = cluster.get_address()
        client = Client(address=address)

        time.sleep(10)
        logging.info("Shutting down")
        client.shutdown()

        time.sleep(5)
        # this is combo cluster, client only shutdown clusters, not scheduler, so scheduler need be shutdown also
        cluster.shutdown()

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

        cluster = SchedulerClusterCombo(
            n_workers=1,
            per_worker_task_queue_size=2,
            event_loop="builtin",
            client_timeout_seconds=client_timeout_seconds,
        )
        address = cluster.get_address()

        try:
            with Client(address, timeout_seconds=client_timeout_seconds) as client:
                future = client.submit(parent, client)
                self.assertEqual(future.result(), "OK")
        finally:
            cluster.shutdown()
