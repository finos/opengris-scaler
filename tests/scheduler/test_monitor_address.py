import socket
import unittest

from scaler import SchedulerClusterCombo
from scaler.config.constants import DEFAULT_MONITOR_PORT_OFFSET
from scaler.config.types.address import AddressConfig, SocketType
from scaler.scheduler.scheduler import default_monitor_address
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import OS_PICKED_PORT
from tests.utility.utility import logging_test_name

HOST = "127.0.0.1"


class TestDefaultMonitorAddress(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_a_port_zero_scheduler_gets_a_port_zero_monitor(self) -> None:
        """A scheduler binding port 0 lets the OS pick its monitor port too."""
        bind_address = AddressConfig(SocketType.tcp, HOST, OS_PICKED_PORT)
        self.assertEqual(default_monitor_address(bind_address), bind_address)

    def test_a_taken_default_monitor_port_fails_an_explicit_scheduler_port(self) -> None:
        """With an explicit scheduler port, a taken port + 2 fails startup, since monitors connect to port + 2."""
        holder = socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM)
        self.addCleanup(holder.close)
        holder.bind((HOST, OS_PICKED_PORT))
        holder.listen()
        scheduler_port = holder.getsockname()[1] - DEFAULT_MONITOR_PORT_OFFSET

        with self.assertRaises(RuntimeError):
            combo = SchedulerClusterCombo(address=f"tcp://{HOST}:{scheduler_port}", n_workers=1)
            combo.shutdown()
