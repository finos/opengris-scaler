import os
import time
import unittest

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility import get_available_tcp_port, logging_test_name


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
        combo = SchedulerClusterCombo(address=address, n_workers=1, per_worker_queue_size=N_TASKS)

        client = Client(address=address)

        futures = [client.submit(sleep_and_return_pid, 10) for _ in range(N_TASKS)]

        time.sleep(5)

        new_cluster = Cluster(
            address=combo._cluster._address,
            worker_io_threads=1,
            worker_names=[str(i) for i in range(0, N_WORKERS - 1)],
            heartbeat_interval_seconds=combo._cluster._heartbeat_interval_seconds,
            task_timeout_seconds=combo._cluster._task_timeout_seconds,
            death_timeout_seconds=combo._cluster._death_timeout_seconds,
            garbage_collect_interval_seconds=combo._cluster._garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=combo._cluster._trim_memory_threshold_bytes,
            hard_processor_suspend=combo._cluster._hard_processor_suspend,
            event_loop=combo._cluster._event_loop,
            logging_paths=combo._cluster._logging_paths,
            logging_level=combo._cluster._logging_level,
            logging_config_file=combo._cluster._logging_config_file,
        )
        new_cluster.start()

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS)

        client.disconnect()

        new_cluster.terminate()
        combo.shutdown()
