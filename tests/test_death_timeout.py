import logging
import time
import unittest

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.io.config import (DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                              DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                              DEFAULT_IO_THREADS, DEFAULT_TASK_TIMEOUT_SECONDS,
                              DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES)
from scaler.utility.logging.utility import setup_logger
from scaler.utility.zmq_config import ZMQConfig
from tests.utility import get_available_tcp_port, logging_test_name

# This is a manual test because it can loop infinitely if it fails


class TestDeathTimeout(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_no_scheduler(self):
        logging.info("test with no scheduler")
        # Test 1: Spinning up a cluster with no scheduler. Death timeout should apply
        cluster = Cluster(
            address=ZMQConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"),
            worker_io_threads=DEFAULT_IO_THREADS,
            worker_names=["a", "b"],
            heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
            garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
            trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
            task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
            death_timeout_seconds=10,
            hard_processor_suspend=False,
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_level="INFO",
            logging_config_file=None,
        )
        cluster.start()
        time.sleep(15)

    def test_shutdown(self):
        logging.info("test with explicitly shutdown")
        # Test 2: Running the Combo and sending shutdown
        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address, n_workers=2, per_worker_queue_size=2, event_loop="builtin", protected=False
        )
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

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        cluster = SchedulerClusterCombo(
            address=address,
            n_workers=1,
            per_worker_queue_size=2,
            event_loop="builtin",
            client_timeout_seconds=client_timeout_seconds,
        )

        try:
            with Client(address, timeout_seconds=client_timeout_seconds) as client:
                future = client.submit(parent, client)
                self.assertEqual(future.result(), "OK")
        finally:
            cluster.shutdown()
