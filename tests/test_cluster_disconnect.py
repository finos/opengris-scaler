import dataclasses
import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
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
        base_config = self.combo._cluster._cluster_config
        dying_config = dataclasses.replace(
            base_config,
            scheduler_address=self.combo._address,
            object_storage_address=self.combo._object_storage_address,
            preload=None,
            worker_names=WorkerNames(names=["dying_worker"]),
            num_of_workers=1,
            per_worker_capabilities=WorkerCapabilities(capabilities={}),
            logging_paths=DEFAULT_LOGGING_PATHS,
        )
        dying_cluster = Cluster(config=dying_config)
        dying_cluster.start()

        client = Client(self.address)
        future_result = client.submit(noop_sleep, 5)
        time.sleep(2)
        dying_cluster.terminate()
        dying_cluster.join()

        with self.assertRaises(CancelledError):
            client.clear()
            future_result.result()
