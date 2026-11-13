import multiprocessing
import socket
import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.config.types.address import AddressConfig
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

# a worker that cannot reach its worker manager never runs the task
TASK_TIMEOUT_SECONDS = 30


class TestCluster(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_manual_storage_address(self):
        N_TASKS = 8
        N_WORKERS = N_TASKS

        combo = SchedulerClusterCombo(
            address="tcp://127.0.0.1:0", object_storage_address="tcp://127.0.0.1:0", n_workers=N_WORKERS
        )

        with Client(address=combo.get_address()) as client:
            future = client.submit(round, 3.14)
            self.assertEqual(future.result(), round(3.14))

        combo.shutdown()

    def test_workers_dial_the_port_their_worker_manager_bound(self):
        """The worker manager binds its children address on port 0, and its workers reach it and run a task."""
        combo = SchedulerClusterCombo(n_workers=1)
        try:
            with Client(address=combo.get_address()) as client:
                self.assertEqual(client.submit(abs, -3).result(timeout=TASK_TIMEOUT_SECONDS), 3)
        finally:
            combo.shutdown()

    def test_scheduler_listens_once_constructed(self):
        """The combo returns only after its scheduler accepts connections, so a client with a short timeout fits."""
        combo = SchedulerClusterCombo(n_workers=1)
        try:
            port = AddressConfig.from_string(combo.get_address()).port
            socket.create_connection(("127.0.0.1", port)).close()
        finally:
            combo.shutdown()

    def test_failed_startup_stops_started_processes(self):
        """A scheduler that fails to bind makes the combo raise and stop the object storage server it started."""
        with socket.socket() as taken:
            taken.bind(("127.0.0.1", 0))
            taken.listen()
            port = taken.getsockname()[1]

            with self.assertRaises(RuntimeError):
                SchedulerClusterCombo(address=f"tcp://127.0.0.1:{port}", n_workers=1)

        leaked = multiprocessing.active_children()
        for process in leaked:
            process.terminate()
            process.join()
        self.assertEqual(leaked, [])
