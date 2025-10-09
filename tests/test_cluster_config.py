import os
import tempfile
import unittest
from argparse import Namespace

from scaler.config.loader import load_config
from scaler.config.section.cluster import ClusterConfig


class TestClusterConfig(unittest.TestCase):
    """Tests for the ClusterConfig class."""

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, "cluster.toml")
        with open(self.config_path, "w") as f:
            f.write(
                """
scheduler_address = "tcp://127.0.0.1:5555"
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
