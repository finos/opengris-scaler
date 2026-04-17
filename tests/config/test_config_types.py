import unittest

from scaler.config.types.address import AddressConfig
from scaler.config.types.http import HTTPConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames


class TestConfigTypes(unittest.TestCase):
    """Tests for individual ConfigType helper classes."""

    def test_http_config_from_string(self):
        """Test HTTPConfig.from_string parses host:port correctly."""
        cfg = HTTPConfig.from_string("0.0.0.0:50001")
        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertEqual(cfg.port, 50001)
        self.assertEqual(str(cfg), "0.0.0.0:50001")

    def test_http_config_ipv6(self):
        """Test HTTPConfig.from_string handles IPv6 addresses via rpartition."""
        cfg = HTTPConfig.from_string("::1:8080")
        self.assertEqual(cfg.port, 8080)

    def test_http_config_validation(self):
        """Test HTTPConfig.from_string raises ValueError for malformed strings."""
        with self.assertRaises(ValueError):
            HTTPConfig.from_string("no-port-here")
        with self.assertRaises(ValueError):
            HTTPConfig.from_string("0.0.0.0:notanumber")

    def test_address_config_validation(self):
        """Test AddressConfig.from_string raises ValueError for malformed strings."""
        with self.assertRaises(ValueError):
            AddressConfig.from_string("this-is-not-a-valid-address")
        with self.assertRaises(ValueError):
            AddressConfig.from_string("tcp://127.0.0.1")
        with self.assertRaises(ValueError):
            AddressConfig.from_string("badprotocol://127.0.0.1:1234")

        cfg = AddressConfig.from_string("ipc://a-valid-path")
        self.assertEqual(cfg.host, "a-valid-path")

    def test_worker_names_config_value(self):
        """Test the WorkerNames ConfigType class."""
        wn = WorkerNames.from_string(" worker1 , worker2 ")
        self.assertEqual(wn.names, ["worker1", "worker2"])
        self.assertEqual(str(wn), "worker1,worker2")
        self.assertEqual(len(wn), 2)
        wn_empty = WorkerNames.from_string("")
        self.assertEqual(wn_empty.names, [])

    def test_worker_capabilities_config_value(self):
        """Test the WorkerCapabilities ConfigType class."""
        wc = WorkerCapabilities.from_string(" gpu=2, linux ")
        self.assertEqual(wc.capabilities, {"gpu": 2, "linux": -1})
        self.assertIn("gpu=2", str(wc))
        self.assertIn("linux", str(wc))

    def test_worker_capabilities_invalid_input(self):
        """Test that WorkerCapabilities raises an error for non-integer values."""
        with self.assertRaises(ValueError):
            WorkerCapabilities.from_string("gpu=two")

    def test_worker_capabilities_invalid_value_in_string(self):
        """Test that WorkerCapabilities.from_string raises a helpful ValueError for non-integer values."""
        with self.assertRaisesRegex(ValueError, "Expected an integer, but got 'MostPowerful'"):
            WorkerCapabilities.from_string("linux,cpu=MostPowerful")


if __name__ == "__main__":
    unittest.main()
