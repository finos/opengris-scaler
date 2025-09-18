import unittest
import os
import tempfile
import tomli as tomllib
from argparse import Namespace
import logging

from scaler.config import ScalerConfig, ObjectStorageConfig, ClusterConfig, defaults
from scaler.utility.zmq_config import ZMQConfig


class TestConfig(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "test_config.toml")
        with open(self.config_path, "w") as f:
            f.write(
                """
[scheduler]
address_str = "tcp://127.0.0.1:9999"
zmq_io_threads = 2
allocate_policy_str = "even"

[cluster]
num_of_workers = 10
"""
            )

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_load_from_toml_only(self):
        """Test loading from TOML without any overrides."""
        config = ScalerConfig.get_config(self.config_path)

        self.assertEqual(ZMQConfig.to_address(config.scheduler.address), "tcp://127.0.0.1:9999")
        self.assertEqual(config.cluster.num_of_workers, 10)
        self.assertEqual(config.scheduler.zmq_io_threads, 2)

    def test_defaults_are_applied(self):
        """Test that defaults are used when not in the TOML file."""
        config = ScalerConfig.get_config(self.config_path)
        self.assertTrue(config.scheduler.protected)
        self.assertEqual(config.worker.heartbeat_interval_seconds, 2)

    def test_command_line_overrides_toml(self):
        """Test that unprefixed (unique) args correctly override TOML values."""
        args = Namespace(scheduler_address_str="tcp://localhost:1111", num_of_workers=20)
        config = ScalerConfig.get_config(self.config_path, args)

        self.assertEqual(ZMQConfig.to_address(config.scheduler.address), "tcp://localhost:1111")
        self.assertEqual(config.cluster.num_of_workers, 20)
        self.assertEqual(config.scheduler.zmq_io_threads, 2)

    def test_describe_method_output(self):
        """Test the describe() method for correct output format."""
        config = ScalerConfig.get_config(self.config_path)
        description = config.describe()
        self.assertIn('scheduler.address = "tcp://127.0.0.1:9999"', description)
        self.assertIn('cluster.num_of_workers = "10"', description)

    def test_malformed_toml_raises_error(self):
        """Test that a TOML file with a syntax error raises TOMLDecodeError."""
        malformed_path = os.path.join(self.temp_dir.name, "bad.toml")
        with open(malformed_path, "w") as f:
            f.write("[scheduler]\naddress_str = tcp://127.0.0.1:9999")
        with self.assertRaises(tomllib.TOMLDecodeError):
            ScalerConfig.get_config(malformed_path)

    def test_non_existent_config_file_uses_defaults(self):
        """Test that a warning is logged and defaults are used when the config file is not found."""
        non_existent_path = os.path.join(self.temp_dir.name, "no_such_file.toml")
        with self.assertLogs(level="WARNING") as cm:
            config = ScalerConfig.get_config(non_existent_path)
            self.assertIn("Configuration file not found", cm.output[0])
            self.assertTrue(config.scheduler.protected)
            self.assertEqual(config.worker.name, "DefaultWorker")

    def test_args_only_overrides_defaults(self):
        """Test that args override defaults when no config file is provided."""
        args = Namespace(scheduler_zmq_io_threads=5, web_port=8888)
        config = ScalerConfig.get_config(filename=None, args=args)

        # Check that the arguments correctly override the defaults
        self.assertEqual(config.scheduler.zmq_io_threads, 5)
        self.assertEqual(config.webui.web_port, 8888)

        # Check that other defaults remain untouched
        self.assertEqual(config.cluster.num_of_workers, defaults.DEFAULT_NUMBER_OF_WORKER)
        self.assertEqual(config.worker.name, "DefaultWorker")

    def test_unprefixed_unique_argument_works(self):
        """Test that a unique argument like 'web_port' works without a prefix."""
        args = Namespace(web_port=8888)
        config = ScalerConfig.get_config(filename=None, args=args)
        self.assertEqual(config.webui.web_port, 8888)

    def test_prefixed_argument_still_works(self):
        """Test that explicitly prefixing an argument still works correctly."""
        args = Namespace(cluster_num_of_workers=50)
        config = ScalerConfig.get_config(filename=None, args=args)
        self.assertEqual(config.cluster.num_of_workers, 50)

    def test_unique_unprefixed_name_argument_works(self):
        """Test a unique, unprefixed argument 'name' is assigned correctly."""
        args = Namespace(name="MyCustomWorker")
        config = ScalerConfig.get_config(filename=None, args=args)
        self.assertEqual(config.worker.name, "MyCustomWorker")

    def test_unknown_field_in_toml_raises_error(self):
        """Test that an unknown field in the TOML file raises a TypeError."""
        extra_field_path = os.path.join(self.temp_dir.name, "extra.toml")
        with open(extra_field_path, "w") as f:
            f.write("[scheduler]\nthis_is_not_a_real_field = true")
        with self.assertRaises(TypeError):
            ScalerConfig.get_config(extra_field_path)

    def test_object_storage_from_string_validation(self):
        """Test the validation in ObjectStorageConfig.from_string raises ValueError."""
        with self.assertRaises(ValueError):
            ObjectStorageConfig.from_string("this-is-not-a-valid-address")
        with self.assertRaises(ValueError):
            ObjectStorageConfig.from_string("tcp://127.0.0.1")

    def test_cluster_config_post_init(self):
        """Directly test the __post_init__ logic of ClusterConfig."""
        conf1 = ClusterConfig(worker_names_str=" worker1 , worker2 ")
        self.assertEqual(conf1.worker_names, ["worker1", "worker2"])
        conf2 = ClusterConfig(worker_names_str="")
        self.assertEqual(conf2.worker_names, [])


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    unittest.main()
