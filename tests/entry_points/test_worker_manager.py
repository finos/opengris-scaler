import dataclasses
import unittest
from typing import Optional
from unittest.mock import mock_open, patch

from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class _LeafConfig(ConfigClass):
    value: int = 0
    name: str = "default"


@dataclasses.dataclass
class _RootConfig(ConfigClass):
    foo: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="foo_section"))
    bar: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="bar_section"))


@dataclasses.dataclass
class _RootWithCommonConfig(ConfigClass):
    log_level: str = "INFO"
    foo: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="foo_section"))
    bar: Optional[_LeafConfig] = dataclasses.field(default=None, metadata=dict(subcommand="bar_section"))


# Two-level nested subcommands for nesting tests
@dataclasses.dataclass
class _Level2Config(ConfigClass):
    depth: int = 2


@dataclasses.dataclass
class _Level1Config(ConfigClass):
    inner: Optional[_Level2Config] = dataclasses.field(default=None, metadata=dict(subcommand="level2_section"))


@dataclasses.dataclass
class _NestedRootConfig(ConfigClass):
    level1: Optional[_Level1Config] = dataclasses.field(default=None, metadata=dict(subcommand="level1_section"))


class TestWorkerManagerSubcommands(unittest.TestCase):
    """Tests the subcommand= metadata path in ConfigClass."""

    @patch("sys.argv", ["prog", "foo", "--value", "42"])
    def test_foo_subcommand_selected(self) -> None:
        config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertIsNone(config.bar)
        self.assertEqual(config.foo.value, 42)

    @patch("sys.argv", ["prog", "bar", "--value", "7"])
    def test_bar_subcommand_selected(self) -> None:
        config = _RootConfig.parse("prog", "")
        self.assertIsNone(config.foo)
        self.assertIsNotNone(config.bar)
        self.assertEqual(config.bar.value, 7)

    @patch("sys.argv", ["prog", "foo"])
    def test_default_values_used(self) -> None:
        config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertEqual(config.foo.value, 0)
        self.assertEqual(config.foo.name, "default")

    @patch("sys.argv", ["prog", "foo", "--log-level", "DEBUG"])
    def test_root_level_fields_populated(self) -> None:
        config = _RootWithCommonConfig.parse("prog", "")
        self.assertEqual(config.log_level, "DEBUG")
        self.assertIsNotNone(config.foo)

    @patch("sys.argv", ["prog", "foo", "--value", "5"])
    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [foo_section]
            value = 99
            name = "from_toml"
            """),
    )
    def test_cli_overrides_toml(self) -> None:
        with patch("sys.argv", ["prog", "--config", "cfg.toml", "foo", "--value", "5"]):
            config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        # CLI --value 5 should override TOML value 99
        self.assertEqual(config.foo.value, 5)
        # name not provided on CLI → TOML value used
        self.assertEqual(config.foo.name, "from_toml")

    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [foo_section]
            value = 77
            """),
    )
    def test_config_after_subcommand(self) -> None:
        """--config appearing after the sub-command name must still be loaded."""
        with patch("sys.argv", ["prog", "foo", "--config", "cfg.toml"]):
            config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertEqual(config.foo.value, 77)

    @patch(
        "builtins.open",
        mock_open(read_data=b"""
            [foo_section]
            value = 55
            """),
    )
    def test_config_before_subcommand(self) -> None:
        """--config appearing before the sub-command name must still be loaded."""
        with patch("sys.argv", ["prog", "--config", "cfg.toml", "foo"]):
            config = _RootConfig.parse("prog", "")
        self.assertIsNotNone(config.foo)
        self.assertEqual(config.foo.value, 55)

    @patch("sys.argv", ["prog"])
    def test_no_subcommand_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _RootConfig.parse("prog", "")

    @patch("sys.argv", ["prog", "bad_cmd"])
    def test_unknown_subcommand_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _RootConfig.parse("prog", "")

    @patch("sys.argv", ["prog", "--help"])
    def test_help_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _RootConfig.parse("prog", "")

    @patch("sys.argv", ["prog", "level1", "inner", "--depth", "99"])
    def test_nested_subcommands_route_correctly(self) -> None:
        config = _NestedRootConfig.parse("prog", "")
        self.assertIsNotNone(config.level1)
        self.assertIsNotNone(config.level1.inner)
        self.assertEqual(config.level1.inner.depth, 99)

    @patch("sys.argv", ["prog", "level1", "inner"])
    def test_nested_subcommands_unselected_are_none(self) -> None:
        config = _NestedRootConfig.parse("prog", "")
        self.assertIsNotNone(config.level1)
        self.assertIsNotNone(config.level1.inner)
        self.assertEqual(config.level1.inner.depth, 2)  # default


# ---------------------------------------------------------------------------
# Tests for the real _WorkerManagerConfig fields
# ---------------------------------------------------------------------------

_NATIVE_BASE_ARGS = [
    "scaler_worker_manager",
    "baremetal_native",
    "--worker-manager-id",
    "wm-test",
    "tcp://127.0.0.1:6378",
]


class TestWorkerManagerConfigFields(unittest.TestCase):
    """Tests that WorkerManagerConfig parses per-manager logging and worker fields from the CLI."""

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--logging-level", "DEBUG"])
    def test_logging_level_from_cli(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertEqual(config.baremetal_native.logging_config.level, "DEBUG")

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--logging-paths", "/tmp/scaler.log"])
    def test_logging_paths_from_cli(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertIn("/tmp/scaler.log", config.baremetal_native.logging_config.paths)

    @patch("sys.argv", _NATIVE_BASE_ARGS)
    def test_logging_defaults(self) -> None:
        from scaler.config.common.logging import LoggingConfig
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertEqual(config.baremetal_native.logging_config.level, LoggingConfig().level)

    def test_logging_level_from_toml(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        toml_content = b"""
[worker_manager_baremetal_native]
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-test"
logging_level = "DEBUG"
"""
        with patch(
            "sys.argv",
            [
                "scaler_worker_manager",
                "--config",
                "cfg.toml",
                "baremetal_native",
                "tcp://127.0.0.1:6378",
                "--worker-manager-id",
                "wm-test",
            ],
        ):
            with patch("builtins.open", mock_open(read_data=toml_content)):
                config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertEqual(config.baremetal_native.logging_config.level, "DEBUG")

    def test_cli_overrides_toml_logging(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        toml_content = b"""
[worker_manager_baremetal_native]
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-test"
logging_level = "DEBUG"
"""
        with patch(
            "sys.argv",
            [
                "scaler_worker_manager",
                "--config",
                "cfg.toml",
                "baremetal_native",
                "--logging-level",
                "WARNING",
                "tcp://127.0.0.1:6378",
                "--worker-manager-id",
                "wm-test",
            ],
        ):
            with patch("builtins.open", mock_open(read_data=toml_content)):
                config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertEqual(config.baremetal_native.logging_config.level, "WARNING")

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--io-threads", "4"])
    def test_worker_io_threads_from_cli(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertIsNotNone(config.baremetal_native)
        self.assertEqual(config.baremetal_native.worker_config.io_threads, 4)

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--event-loop", "builtin"])
    def test_event_loop_from_cli(self) -> None:
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertIsNotNone(config.baremetal_native)
        self.assertEqual(config.baremetal_native.worker_config.event_loop, "builtin")

    @patch("sys.argv", _NATIVE_BASE_ARGS)
    def test_per_manager_config_defaults(self) -> None:
        from scaler.config.common.worker import WorkerConfig
        from scaler.entry_points.worker_manager import WorkerManagerConfig

        config = WorkerManagerConfig.parse("scaler_worker_manager", "")
        self.assertEqual(config.baremetal_native.worker_config.io_threads, WorkerConfig().io_threads)
        self.assertEqual(config.baremetal_native.worker_config.event_loop, WorkerConfig().event_loop)
