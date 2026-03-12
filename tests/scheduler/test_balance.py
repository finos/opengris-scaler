import os
import time
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import DEFAULT_LOAD_BALANCE_SECONDS
from scaler.config.section.fixed_native_worker_manager import FixedNativeWorkerManagerConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_manager_adapter.baremetal.fixed_native import FixedNativeWorkerManager
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

        base_manager = combo._worker_manager
        new_manager = FixedNativeWorkerManager(
            FixedNativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=base_manager._address, object_storage_address=None, max_workers=N_WORKERS - 1
                ),
                preload=None,
                event_loop=base_manager._event_loop,
                worker_io_threads=1,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=base_manager._task_queue_size,
                    heartbeat_interval_seconds=base_manager._heartbeat_interval_seconds,
                    task_timeout_seconds=base_manager._task_timeout_seconds,
                    death_timeout_seconds=base_manager._death_timeout_seconds,
                    garbage_collect_interval_seconds=base_manager._garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=base_manager._trim_memory_threshold_bytes,
                    hard_processor_suspend=base_manager._hard_processor_suspend,
                ),
                logging_config=LoggingConfig(
                    paths=base_manager._logging_paths,
                    level=base_manager._logging_level,
                    config_file=base_manager._logging_config_file,
                ),
            )
        )
        new_manager.start()

        pids = {f.result() for f in futures}

        self.assertEqual(len(pids), N_WORKERS)

        client.disconnect()

        new_manager.shutdown()
        combo.shutdown()
