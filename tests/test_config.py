import os
import tempfile
import unittest
from argparse import Namespace

from scaler.config.section.cluster import ClusterConfig
from scaler.config.loader import load_config
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.config.types.zmq import ZMQConfig
from scaler.config import defaults
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy


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


class TestSchedulerConfig(unittest.TestCase):
    """Tests for the SchedulerConfig class."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "scheduler.toml")
        with open(self.config_path, "w") as f:
            f.write(
                """
scheduler_address = "tcp://127.0.0.1:9999"
io_threads = 2
allocate_policy = "even"
"""
            )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_from_toml_only(self):
        """Test loading SchedulerConfig from a flat TOML file."""
        config = load_config(SchedulerConfig, self.config_path, Namespace())
        self.assertEqual(str(config.scheduler_address), "tcp://127.0.0.1:9999")
        self.assertEqual(config.io_threads, 2)
        self.assertEqual(config.allocate_policy, AllocatePolicy.even)

    def test_defaults_are_applied(self):
        """Test that defaults are used when not in the TOML file."""
        config = load_config(SchedulerConfig, self.config_path, Namespace())
        self.assertTrue(config.protected)
        self.assertEqual(config.client_timeout_seconds, defaults.DEFAULT_CLIENT_TIMEOUT_SECONDS)

    def test_command_line_overrides_toml(self):
        """Test that argparse args correctly override TOML values."""
        args = Namespace(scheduler_address="tcp://localhost:1111", io_threads=5)
        config = load_config(SchedulerConfig, self.config_path, args)
        self.assertEqual(str(config.scheduler_address), "tcp://localhost:1111")
        self.assertEqual(config.io_threads, 5)
        self.assertEqual(config.allocate_policy, AllocatePolicy.even)

    def test_non_existent_config_file_raises_error(self):
        """Test that a non-existent config file raises FileNotFoundError."""
        non_existent_path = os.path.join(self.temp_dir.name, "no_such_file.toml")
        with self.assertRaises(FileNotFoundError):
            load_config(SchedulerConfig, non_existent_path, Namespace())

    def test_unknown_field_in_toml_raises_error(self):
        """Test that an unknown field in the TOML file raises a ValueError."""
        extra_field_path = os.path.join(self.temp_dir.name, "extra.toml")
        with open(extra_field_path, "w") as f:
            f.write("this_is_not_a_real_field = true")
        with self.assertRaises(ValueError):
            load_config(SchedulerConfig, extra_field_path, Namespace())

    def test_optional_field_handling(self):
        """Test that an Optional field is None by default and can be set."""
        config_no_storage = load_config(SchedulerConfig, self.config_path, Namespace())
        self.assertIsNone(config_no_storage.object_storage_address)

        optional_toml_path = os.path.join(self.temp_dir.name, "optional.toml")
        with open(optional_toml_path, "w") as f:
            f.write('scheduler_address = "tcp://127.0.0.1:1234"\nobject_storage_address = "tcp://127.0.0.1:5678"')
        config_with_storage = load_config(SchedulerConfig, optional_toml_path, Namespace())
        self.assertIsInstance(config_with_storage.object_storage_address, ObjectStorageConfig)
        self.assertEqual(config_with_storage.object_storage_address.port, 5678)

    def test_invalid_enum_in_toml_raises_error(self):
        """Test that a bad enum value in TOML raises a ValueError."""
        bad_enum_path = os.path.join(self.temp_dir.name, "bad_enum.toml")
        with open(bad_enum_path, "w") as f:
            f.write('scheduler_address = "tcp://127.0.0.1:1234"\nallocate_policy = "invalid_policy"')
        with self.assertRaises(ValueError):
            load_config(SchedulerConfig, bad_enum_path, Namespace())

    def test_list_to_tuple_coercion(self):
        """Test that a list from args is converted to a tuple."""
        args = Namespace(scheduler_address="tcp://127.0.0.1:1234", logging_paths=["/var/log/scaler.log", "/dev/stdout"])
        config = load_config(SchedulerConfig, config_path=None, args=args)
        self.assertIsInstance(config.logging_paths, tuple)
        self.assertEqual(config.logging_paths, ("/var/log/scaler.log", "/dev/stdout"))


class TestClusterConfig(unittest.TestCase):
    """Tests for the ClusterConfig class."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "cluster.toml")
        with open(self.config_path, "w") as f:
            f.write(
                """
scheduler_address = "tcp://127.0.0.1:5555"
# FIX: Changed num_of_workers to match the number of worker_names
num_of_workers = 2
worker_names = "w1,w2"
"""
            )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_from_toml_only(self):
        """Test loading ClusterConfig from a flat TOML file."""
        config = load_config(ClusterConfig, self.config_path, Namespace())
        self.assertEqual(str(config.scheduler_address), "tcp://127.0.0.1:5555")
        self.assertEqual(config.num_of_workers, 2)
        self.assertEqual(config.worker_names.names, ["w1", "w2"])

    def test_command_line_overrides_toml(self):
        """Test that argparse args correctly override TOML values for ClusterConfig."""
        worker_list = [f"worker_{i}" for i in range(20)]
        args = Namespace(num_of_workers=20, worker_names=",".join(worker_list), heartbeat_interval_seconds=99)

        config = load_config(ClusterConfig, self.config_path, args)

        self.assertEqual(config.num_of_workers, 20)
        self.assertEqual(config.worker_names.names, worker_list)
        self.assertEqual(config.heartbeat_interval_seconds, 99)
        self.assertEqual(str(config.scheduler_address), "tcp://127.0.0.1:5555")


if __name__ == "__main__":
    unittest.main()
