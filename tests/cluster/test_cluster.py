import socket
import unittest
from typing import List, Optional

import psutil

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.utility.utility import logging_test_name


class TestCluster(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_manual_storage_address(self):
        N_TASKS = 8
        N_WORKERS = N_TASKS

        address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        object_storage_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        combo = SchedulerClusterCombo(
            address=address, object_storage_address=object_storage_address, n_workers=N_WORKERS
        )

        with Client(address=address) as client:
            future = client.submit(round, 3.14)
            self.assertEqual(future.result(), round(3.14))

        combo.shutdown()

    def test_scheduler_listens_once_constructed(self):
        """The combo returns only after its scheduler accepts connections, so a client with a short timeout fits."""
        port = get_available_tcp_port()
        combo = SchedulerClusterCombo(address=f"tcp://127.0.0.1:{port}", n_workers=1)
        try:
            socket.create_connection(("127.0.0.1", port)).close()
        finally:
            combo.shutdown()


def _running_children() -> List[psutil.Process]:
    # multiprocessing starts its resource tracker on first use and keeps it for the whole run
    return [
        child
        for child in psutil.Process().children(recursive=True)
        if child.is_running() and not any("resource_tracker" in part for part in child.cmdline())
    ]


class TestClusterStartupFailure(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_a_scheduler_that_fails_to_start_leaves_no_process_behind(self) -> None:
        """When the scheduler exits during startup, the combo stops the processes it already started."""
        children_before = {child.pid for child in _running_children()}

        # the scheduler exits with code 1 when its monitor address is taken
        holder = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self.addCleanup(holder.close)
        holder.bind(("127.0.0.1", 0))
        holder.listen()
        monitor_port = holder.getsockname()[1]

        # keep the traceback, as a failed test's report does, so the combo's __del__ cannot do the cleanup
        startup_failure: Optional[RuntimeError] = None
        try:
            SchedulerClusterCombo(n_workers=0, monitor_address=f"tcp://127.0.0.1:{monitor_port}")
        except RuntimeError as error:
            startup_failure = error

        leaked = [child for child in _running_children() if child.pid not in children_before]
        self.assertEqual(leaked, [])
        self.assertIn("Scheduler exited", str(startup_failure))
