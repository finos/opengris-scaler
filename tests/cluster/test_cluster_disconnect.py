import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.config.section.cluster import ClusterConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name


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
            config=ClusterConfig(
                scheduler_address=self.combo._address,
                object_storage_address=self.combo._object_storage_address,
                preload=None,
                worker_names=WorkerNames(["dying_worker"]),  # Just one worker would suffice
                num_of_workers=1,
                event_loop=base_cluster._event_loop,
                worker_io_threads=base_cluster._worker_io_threads,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=base_cluster._per_worker_task_queue_size,
                    heartbeat_interval_seconds=base_cluster._heartbeat_interval_seconds,
                    task_timeout_seconds=base_cluster._task_timeout_seconds,
                    death_timeout_seconds=base_cluster._death_timeout_seconds,
                    garbage_collect_interval_seconds=base_cluster._garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=base_cluster._trim_memory_threshold_bytes,
                    hard_processor_suspend=base_cluster._hard_processor_suspend,
                ),
                logging_config=LoggingConfig(
                    paths=DEFAULT_LOGGING_PATHS,
                    level=base_cluster._logging_level,
                    config_file=base_cluster._logging_config_file,
                ),
            )
        )
        dying_cluster.start()

        client = Client(self.address)
        future_result = client.submit(noop_sleep, 5)
        time.sleep(2)
        dying_cluster.shutdown()

        with self.assertRaises(CancelledError):
            client.clear()
            future_result.result()
