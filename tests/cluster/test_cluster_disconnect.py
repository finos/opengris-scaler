import multiprocessing
import time
import unittest
from concurrent.futures import CancelledError

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.defaults import DEFAULT_LOGGING_PATHS
from scaler.config.section.native_worker_adapter import NativeWorkerManagerConfig, NativeWorkerManagerMode
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerAdapter
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
        base_adapter = self.combo._worker_adapter
        dying_adapter = NativeWorkerAdapter(
            NativeWorkerManagerConfig(
                worker_adapter_config=WorkerAdapterConfig(
                    scheduler_address=self.combo._address,
                    object_storage_address=self.combo._object_storage_address,
                    max_workers=1,
                ),
                preload=None,
                event_loop=base_adapter._event_loop,
                worker_io_threads=base_adapter._io_threads,
                mode=NativeWorkerManagerMode.FIXED,
                worker_config=WorkerConfig(
                    per_worker_capabilities=WorkerCapabilities({}),
                    per_worker_task_queue_size=base_adapter._task_queue_size,
                    heartbeat_interval_seconds=base_adapter._heartbeat_interval_seconds,
                    task_timeout_seconds=base_adapter._task_timeout_seconds,
                    death_timeout_seconds=base_adapter._death_timeout_seconds,
                    garbage_collect_interval_seconds=base_adapter._garbage_collect_interval_seconds,
                    trim_memory_threshold_bytes=base_adapter._trim_memory_threshold_bytes,
                    hard_processor_suspend=base_adapter._hard_processor_suspend,
                ),
                logging_config=LoggingConfig(
                    paths=DEFAULT_LOGGING_PATHS,
                    level=base_adapter._logging_level,
                    config_file=base_adapter._logging_config_file,
                ),
            )
        )
        dying_process = multiprocessing.Process(target=dying_adapter.run)
        dying_process.start()

        client = Client(self.address)
        future_result = client.submit(noop_sleep, 5)
        time.sleep(2)
        dying_process.terminate()
        dying_process.join()

        with self.assertRaises(CancelledError):
            client.clear()
            future_result.result()
