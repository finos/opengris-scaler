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
# Tests for the scaler_worker_manager subcommand interface
# ---------------------------------------------------------------------------

_NATIVE_BASE_ARGS = [
    "scaler_worker_manager",
    "baremetal_native",
    "--worker-manager-id",
    "wm-test",
    "tcp://127.0.0.1:6378",
]


class TestWorkerManagerConfigFields(unittest.TestCase):
    """Tests that the subcommand interface correctly parses per-manager fields from CLI and TOML."""

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--logging-level", "DEBUG"])
    def test_logging_level_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--logging-level", "DEBUG"],
        )
        self.assertEqual(config.logging_config.level, "DEBUG")

    @patch("sys.argv", [*_NATIVE_BASE_ARGS, "--logging-paths", "/tmp/scaler.log"])
    def test_logging_paths_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--logging-paths", "/tmp/scaler.log"],
        )
        self.assertIn("/tmp/scaler.log", config.logging_config.paths)

    def test_logging_defaults(self) -> None:
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378"]
        )
        self.assertEqual(config.logging_config.level, LoggingConfig().level)

    def test_logging_level_from_toml(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        section_data = {
            "type": "baremetal_native",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "logging_level": "DEBUG",
        }
        config = NativeWorkerManagerConfig.parse_with_section("scaler_worker_manager", section_data, argv=[])
        self.assertEqual(config.logging_config.level, "DEBUG")

    def test_cli_overrides_toml_logging(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        section_data = {
            "type": "baremetal_native",
            "scheduler_address": "tcp://127.0.0.1:6378",
            "worker_manager_id": "wm-test",
            "logging_level": "DEBUG",
        }
        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", section_data, argv=["--logging-level", "WARNING"]
        )
        self.assertEqual(config.logging_config.level, "WARNING")

    def test_worker_io_threads_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--io-threads", "4"],
        )
        self.assertEqual(config.worker_config.io_threads, 4)

    def test_event_loop_from_cli(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378", "--event-loop", "builtin"],
        )
        self.assertEqual(config.worker_config.event_loop, "builtin")

    def test_per_manager_config_defaults(self) -> None:
        from scaler.config.common.worker import WorkerConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        config = NativeWorkerManagerConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=["--worker-manager-id", "wm-test", "tcp://127.0.0.1:6378"]
        )
        self.assertEqual(config.worker_config.io_threads, WorkerConfig().io_threads)
        self.assertEqual(config.worker_config.event_loop, WorkerConfig().event_loop)


_ORB_AWS_EC2_BASE_ARGV = ["tcp://127.0.0.1:6378", "--image-id", "ami-0528819f94f4f5fa5"]


class TestORBAWSEC2WorkerManagerSubcommand(unittest.TestCase):
    """Tests that ORBAWSEC2WorkerAdapterConfig is correctly parsed via parse_with_section."""

    def test_orb_aws_ec2_image_id_parsed(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig

        config = ORBAWSEC2WorkerAdapterConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=_ORB_AWS_EC2_BASE_ARGV
        )
        self.assertIsInstance(config, ORBAWSEC2WorkerAdapterConfig)
        self.assertEqual(config.image_id, "ami-0528819f94f4f5fa5")

    def test_orb_aws_ec2_defaults(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig

        config = ORBAWSEC2WorkerAdapterConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=_ORB_AWS_EC2_BASE_ARGV
        )
        self.assertEqual(config.instance_type, "t2.micro")
        self.assertEqual(config.aws_region, "us-east-1")

    def test_orb_aws_ec2_instance_type_and_region_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig

        config = ORBAWSEC2WorkerAdapterConfig.parse_with_section(
            "scaler_worker_manager",
            {},
            argv=[*_ORB_AWS_EC2_BASE_ARGV, "--instance-type", "t3.medium", "--aws-region", "eu-west-1"],
        )
        self.assertEqual(config.instance_type, "t3.medium")
        self.assertEqual(config.aws_region, "eu-west-1")

    def test_orb_aws_ec2_logging_level_from_cli(self) -> None:
        from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig

        config = ORBAWSEC2WorkerAdapterConfig.parse_with_section(
            "scaler_worker_manager", {}, argv=[*_ORB_AWS_EC2_BASE_ARGV, "--logging-level", "DEBUG"]
        )
        self.assertEqual(config.logging_config.level, "DEBUG")


class TestWorkerManagerMain(unittest.TestCase):
    """Tests for the main() entry point dispatch and error handling."""

    def test_no_matching_type_exits(self) -> None:
        """When --config is provided but has no matching type, exit with error."""
        toml_content = b"""
[[worker_manager]]
type = "symphony"
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-sym"
service_name = "svc"
"""
        with patch("builtins.open", mock_open(read_data=toml_content)):
            with patch("sys.argv", ["scaler_worker_manager", "baremetal_native", "--config", "cfg.toml"]):
                from scaler.entry_points.worker_manager import main

                with self.assertRaises(SystemExit) as ctx:
                    main()
                self.assertEqual(ctx.exception.code, 1)

    def test_multiple_matching_types_exits(self) -> None:
        """When config has two entries of the same type, exit with error."""
        toml_content = b"""
[[worker_manager]]
type = "baremetal_native"
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-1"

[[worker_manager]]
type = "baremetal_native"
scheduler_address = "tcp://127.0.0.1:6378"
worker_manager_id = "wm-2"
"""
        with patch("builtins.open", mock_open(read_data=toml_content)):
            with patch("sys.argv", ["scaler_worker_manager", "baremetal_native", "--config", "cfg.toml"]):
                from scaler.entry_points.worker_manager import main

                with self.assertRaises(SystemExit) as ctx:
                    main()
                self.assertEqual(ctx.exception.code, 1)

    def test_unknown_type_exits(self) -> None:
        """Unknown subcommand exits with code 1."""
        with patch("sys.argv", ["scaler_worker_manager", "nonexistent"]):
            from scaler.entry_points.worker_manager import main

            with self.assertRaises(SystemExit) as ctx:
                main()
            self.assertEqual(ctx.exception.code, 1)
