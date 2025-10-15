import unittest

from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.config.types.zmq import ZMQConfig


class TestConfigTypes(unittest.TestCase):
    """Tests for individual ConfigType helper classes."""

    def test_zmq_config_validation(self):
        """Test ZMQConfig.from_string raises ValueError for malformed strings."""
        with self.assertRaises(ValueError):
            ZMQConfig.from_string("this-is-not-a-valid-address")
        with self.assertRaises(ValueError):
            ZMQConfig.from_string("tcp://127.0.0.1")
        with self.assertRaises(ValueError):
            ZMQConfig.from_string("badprotocol://127.0.0.1:1234")

        cfg = ZMQConfig.from_string("ipc://a-valid-path")
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
