import dataclasses
import unittest
from concurrent.futures import TimeoutError

from scaler import Client, Cluster, SchedulerClusterCombo
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.utility.logging.utility import setup_logger
from tests.utility import logging_test_name


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
        base_config = self.combo._cluster._cluster_config

        with Client(self.address) as client:
            client.submit(round, 3.14).result()  # Ensures the cluster is ready

            future = client.submit_verbose(round, args=(3.14,), kwargs={}, capabilities={"gpu": 1})

            # No worker can accept the task, should timeout
            with self.assertRaises(TimeoutError):
                future.result(timeout=1)

            gpu_config = dataclasses.replace(
                base_config,
                object_storage_address=None,
                preload=None,
                worker_names=WorkerNames(names=["gpu_worker"]),
                num_of_workers=1,
                per_worker_capabilities=WorkerCapabilities(capabilities={"gpu": -1}),
                worker_io_threads=1,
            )

            # Connects a worker that can handle the task
            gpu_cluster = Cluster(config=gpu_config)
            gpu_cluster.start()

            self.assertEqual(future.result(), 3.0)

            gpu_cluster.terminate()

    def test_graph_capabilities(self):
        base_config = self.combo._cluster._cluster_config

        with Client(self.address) as client:
            client.submit(round, 3.14).result()  # Ensures the cluster is ready

            graph = {"a": 2.3, "b": 3.1, "c": (round, "a"), "d": (round, "b"), "e": (pow, "c", "d")}

            future = client.get(graph, keys=["e"], capabilities={"gpu": 1}, block=False)["e"]

            with self.assertRaises(TimeoutError):
                future.result(timeout=1)

            gpu_config = dataclasses.replace(
                base_config,
                preload=None,
                object_storage_address=None,
                worker_names=WorkerNames(names=["gpu_worker"]),
                num_of_workers=1,
                per_worker_capabilities=WorkerCapabilities(capabilities={"gpu": -1}),
                worker_io_threads=1,
            )
            # Connect a worker that can handle the task
            gpu_cluster = Cluster(config=gpu_config)
            gpu_cluster.start()

            self.assertEqual(future.result(), 8)

            gpu_cluster.terminate()
