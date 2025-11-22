import os
import time
import unittest

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.defaults import DEFAULT_LOAD_BALANCE_SECONDS
from scaler.config.section.cluster import ClusterConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name


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

        new_cluster = Cluster(
            config=ClusterConfig(
                scheduler_address=combo._cluster._address,
                object_storage_address=None,
                preload=None,
                worker_names=WorkerNames([str(i) for i in range(0, N_WORKERS - 1)]),
                num_of_workers=N_WORKERS - 1,
                event_loop=combo._cluster._event_loop,
                worker_io_threads=1,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=combo._cluster._per_worker_task_queue_size,
                    heartbeat_interval_seconds=combo._cluster._heartbeat_interval_seconds,
                    task_timeout_seconds=combo._cluster._task_timeout_seconds,
                    death_timeout_seconds=combo._cluster._death_timeout_seconds,
                    garbage_collect_interval_seconds=combo._cluster._garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=combo._cluster._trim_memory_threshold_bytes,
                    hard_processor_suspend=combo._cluster._hard_processor_suspend,
                ),
                logging_config=LoggingConfig(
                    paths=combo._cluster._logging_paths,
                    level=combo._cluster._logging_level,
                    config_file=combo._cluster._logging_config_file,
                ),
            )
        )
        new_cluster.start()

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS)

        client.disconnect()

        new_cluster.terminate()
        combo.shutdown()
