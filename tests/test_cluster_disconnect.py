import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.utility.logging.utility import setup_logger
from tests.utility import logging_test_name


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
        base_cluster = self.combo._cluster
        dying_cluster = Cluster(
            address=self.combo._address,
            storage_address=self.combo._storage_address,
            preload=None,
            worker_io_threads=base_cluster._worker_io_threads,
            worker_names=["dying_worker"],  # Just one worker would suffice
            per_worker_capabilities={},
            per_worker_task_queue_size=base_cluster._per_worker_task_queue_size,
            heartbeat_interval_seconds=base_cluster._heartbeat_interval_seconds,
            task_timeout_seconds=base_cluster._task_timeout_seconds,
            death_timeout_seconds=base_cluster._death_timeout_seconds,
            garbage_collect_interval_seconds=base_cluster._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=base_cluster._trim_memory_threshold_bytes,
            hard_processor_suspend=base_cluster._hard_processor_suspend,
            event_loop=base_cluster._event_loop,
            logging_paths=DEFAULT_LOGGING_PATHS,
            logging_level=base_cluster._logging_level,
            logging_config_file=base_cluster._logging_config_file,
            transport_type=base_cluster._transport_type,
        )
        dying_cluster.start()

        client = Client(self.address)
        future_result = client.submit(noop_sleep, 5)
        time.sleep(2)
        dying_cluster.terminate()
        dying_cluster.join()

        with self.assertRaises(CancelledError):
            client.clear()
            future_result.result()
