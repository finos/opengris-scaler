import unittest
from concurrent.futures import TimeoutError

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.section.cluster import ClusterConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name


class TestCapabilities(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self._workers = 3
        self.combo = SchedulerClusterCombo(
            n_workers=self._workers, event_loop="builtin", allocate_policy=AllocatePolicy.capability
        )
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_capabilities(self):
        base_cluster = self.combo._cluster

        with Client(self.address) as client:
            client.submit(round, 3.14).result()  # Ensures the cluster is ready

            future = client.submit_verbose(round, args=(3.14,), kwargs={}, capabilities={"gpu": 1})

            # No worker can accept the task, should timeout
            with self.assertRaises(TimeoutError):
                future.result(timeout=1)

            # Connects a worker that can handle the task
            gpu_cluster = Cluster(
                config=ClusterConfig(
                    scheduler_address=base_cluster._address,
                    object_storage_address=None,
                    preload=None,
                    worker_names=WorkerNames(["gpu_worker"]),
                    num_of_workers=1,
                    event_loop=base_cluster._event_loop,
                    worker_io_threads=1,
                    worker_config=WorkerConfig(
                        per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                        per_worker_task_queue_size=base_cluster._per_worker_task_queue_size,
                        heartbeat_interval_seconds=base_cluster._heartbeat_interval_seconds,
                        task_timeout_seconds=base_cluster._task_timeout_seconds,
                        death_timeout_seconds=base_cluster._death_timeout_seconds,
                        garbage_collect_interval_seconds=base_cluster._garbage_collect_interval_seconds,
                        trim_memory_threshold_bytes=base_cluster._trim_memory_threshold_bytes,
                        hard_processor_suspend=base_cluster._hard_processor_suspend,
                    ),
                    logging_config=LoggingConfig(
                        paths=base_cluster._logging_paths,
                        level=base_cluster._logging_level,
                        config_file=base_cluster._logging_config_file,
                    ),
                )
            )
            gpu_cluster.start()

            self.assertEqual(future.result(), 3.0)

            gpu_cluster.terminate()

    def test_graph_capabilities(self):
        base_cluster = self.combo._cluster

        with Client(self.address) as client:
            client.submit(round, 3.14).result()  # Ensures the cluster is ready

            graph = {"a": 2.3, "b": 3.1, "c": (round, "a"), "d": (round, "b"), "e": (pow, "c", "d")}

            future = client.get(graph, keys=["e"], capabilities={"gpu": 1}, block=False)["e"]

            with self.assertRaises(TimeoutError):
                future.result(timeout=1)

            # Connect a worker that can handle the task
            gpu_cluster = Cluster(
                config=ClusterConfig(
                    scheduler_address=base_cluster._address,
                    object_storage_address=None,
                    preload=None,
                    worker_names=WorkerNames(["gpu_worker"]),
                    num_of_workers=1,
                    event_loop=base_cluster._event_loop,
                    worker_io_threads=1,
                    worker_config=WorkerConfig(
                        per_worker_capabilities=WorkerCapabilities({"gpu": -1}),
                        per_worker_task_queue_size=base_cluster._per_worker_task_queue_size,
                        heartbeat_interval_seconds=base_cluster._heartbeat_interval_seconds,
                        task_timeout_seconds=base_cluster._task_timeout_seconds,
                        death_timeout_seconds=base_cluster._death_timeout_seconds,
                        garbage_collect_interval_seconds=base_cluster._garbage_collect_interval_seconds,
                        trim_memory_threshold_bytes=base_cluster._trim_memory_threshold_bytes,
                        hard_processor_suspend=base_cluster._hard_processor_suspend,
                    ),
                    logging_config=LoggingConfig(
                        paths=base_cluster._logging_paths,
                        level=base_cluster._logging_level,
                        config_file=base_cluster._logging_config_file,
                    ),
                )
            )
            gpu_cluster.start()

            self.assertEqual(future.result(), 8)

            gpu_cluster.terminate()
