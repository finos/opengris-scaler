"""
Tests that the example_config.toml shown in the README can be parsed correctly
by each component's config class.

The TOML content is copied verbatim from the README.  Only positional arguments
(addresses) are supplied on the command line, as the README commands show.
"""

import unittest
from unittest.mock import mock_open, patch

from scaler.config.section.native_worker_manager import NativeWorkerManagerMode
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.webgui import WebGUIConfig

README_TOML = b"""
[scheduler]
object_storage_address = "tcp://127.0.0.1:6379"
monitor_address = "tcp://127.0.0.1:6380"
logging_level = "INFO"
logging_paths = ["/dev/stdout", "/var/log/scaler/scheduler.log"]
policy_engine_type = "simple"
policy_content = "allocate=even_load; scaling=no"

[worker_manager_baremetal_native]
mode = "fixed"
max_task_concurrency = 8
worker_manager_id = "my-manager"
per_worker_capabilities = "linux,cpu=8"
task_timeout_seconds = 600
logging_level = "INFO"
logging_paths = ["/dev/stdout", "/var/log/scaler/worker.log"]

[object_storage_server]

[gui]
web_port = 8081
"""


class TestReadmeConfig(unittest.TestCase):
    @patch("sys.argv", ["scaler_scheduler", "tcp://127.0.0.1:6378", "--config", "config.toml"])
    @patch("builtins.open", mock_open(read_data=README_TOML))
    def test_scheduler_section(self) -> None:
        config = SchedulerConfig.parse("scaler_scheduler", "scheduler")

        self.assertEqual(str(config.object_storage_address), "tcp://127.0.0.1:6379")
        self.assertEqual(str(config.monitor_address), "tcp://127.0.0.1:6380")
        self.assertEqual(config.logging_config.level, "INFO")
        self.assertIn("/dev/stdout", config.logging_config.paths)
        self.assertIn("/var/log/scaler/scheduler.log", config.logging_config.paths)
        self.assertEqual(config.policy.policy_engine_type, "simple")
        self.assertEqual(config.policy.policy_content, "allocate=even_load; scaling=no")

    @patch("sys.argv", ["scaler_worker_manager", "baremetal_native", "tcp://127.0.0.1:6378", "--config", "config.toml"])
    @patch("builtins.open", mock_open(read_data=README_TOML))
    def test_baremetal_native_section(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        wm = config.baremetal_native
        self.assertIsNotNone(wm)

        self.assertIsInstance(wm.mode, NativeWorkerManagerMode)
        self.assertEqual(wm.mode, NativeWorkerManagerMode.FIXED)
        self.assertEqual(wm.worker_manager_config.max_task_concurrency, 8)
        self.assertEqual(wm.worker_manager_id, "my-manager")
        self.assertEqual(wm.worker_config.task_timeout_seconds, 600)
        self.assertIn("linux", wm.worker_config.per_worker_capabilities.capabilities)
        self.assertEqual(wm.logging_config.level, "INFO")
        self.assertIn("/dev/stdout", wm.logging_config.paths)
        self.assertIn("/var/log/scaler/worker.log", wm.logging_config.paths)

    @patch("sys.argv", ["scaler_gui", "tcp://127.0.0.1:6380", "--config", "config.toml"])
    @patch("builtins.open", mock_open(read_data=README_TOML))
    def test_webgui_section(self) -> None:
        config = WebGUIConfig.parse("scaler_gui", "gui")

        self.assertEqual(config.web_port, 8081)
