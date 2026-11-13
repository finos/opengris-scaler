import ipaddress
import unittest
from unittest import mock

from scaler.config.types.address import AddressConfig
from scaler.utility.network_util import get_hostname, resolve_port_zero

BOUND = AddressConfig.from_string("tcp://0.0.0.0:45605")


class TestGetHostname(unittest.TestCase):
    def setUp(self) -> None:
        get_hostname.cache_clear()
        self.addCleanup(get_hostname.cache_clear)

    def test_returns_hostname(self) -> None:
        """A real hostname is reported as is."""
        with mock.patch("socket.gethostname", return_value="worker-host"):
            self.assertEqual(get_hostname(), "worker-host")

    def test_localhost_reports_ip(self) -> None:
        """A host named localhost, as in an OCI container instance, reports its non-loopback IP instead."""
        for hostname in ("localhost", "localhost.localdomain", ""):
            with self.subTest(hostname=hostname), mock.patch("socket.gethostname", return_value=hostname):
                get_hostname.cache_clear()
                self.assertFalse(ipaddress.ip_address(get_hostname()).is_loopback)

    def test_localhost_without_route_keeps_hostname(self) -> None:
        """Without a route to pick an IP from, the hostname is reported as is."""
        with (
            mock.patch("socket.gethostname", return_value="localhost"),
            mock.patch("socket.socket.connect", side_effect=OSError("network is unreachable")),
        ):
            self.assertEqual(get_hostname(), "localhost")


class TestResolvePortZero(unittest.TestCase):
    def test_port_zero_takes_the_bound_port_and_keeps_its_host(self) -> None:
        """A connecting address naming port 0 gets the bound port, and its host stays as written."""
        resolved = resolve_port_zero(AddressConfig.from_string("tcp://10.0.0.5:0"), BOUND)
        self.assertEqual(resolved, AddressConfig.from_string("tcp://10.0.0.5:45605"))

    def test_a_named_port_is_left_alone(self) -> None:
        """Only port 0 is resolved: a config naming real ports behaves as before."""
        address = AddressConfig.from_string("tcp://0.0.0.0:8517")
        self.assertEqual(resolve_port_zero(address, BOUND), address)


if __name__ == "__main__":
    unittest.main()
