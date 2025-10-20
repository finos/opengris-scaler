import dataclasses
import os
import time
import unittest

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.defaults import DEFAULT_LOAD_BALANCE_SECONDS
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility import logging_test_name


def sleep_and_return_pid(sec: int):
    time.sleep(sec)
    return os.getpid()


class TestBalance(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_balance(self):
        """
        Schedules a few long-lasting tasks to a single process cluster, then adds workers. We expect the remaining tasks
        to be balanced to the new workers.
        """

        N_TASKS = 8
        N_WORKERS = N_TASKS

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address,
            n_workers=1,
            per_worker_task_queue_size=N_TASKS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
        )

        client = Client(address=address)

        futures = [client.submit(sleep_and_return_pid, 10) for _ in range(N_TASKS)]

        time.sleep(3)

        base_config = combo._cluster._cluster_config

        # Create a new config for the new cluster by replacing fields in the base config
        new_cluster_config = dataclasses.replace(
            base_config,
            object_storage_address=None,
            preload=None,
            worker_io_threads=1,
            worker_names=WorkerNames(names=[str(i) for i in range(0, N_WORKERS - 1)]),
            per_worker_capabilities=WorkerCapabilities(capabilities={}),
            num_of_workers=N_WORKERS - 1,
        )

        new_cluster = Cluster(config=new_cluster_config)
        new_cluster.start()

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS)

        client.disconnect()

        new_cluster.terminate()
        combo.shutdown()
