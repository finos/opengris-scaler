import dataclasses
import unittest
from typing import List, Optional
from unittest.mock import patch

from scaler.config.config_class import ConfigClass
from scaler.config.reconstruction import _from_args

# ---------------------------------------------------------------------------
# Minimal stub configs for section= tests
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _SimpleSchedulerConfig(ConfigClass):
    host: str = "localhost"
    port: int = 8516


@dataclasses.dataclass
class _SimpleWorkerConfig(ConfigClass):
    workers: int = 1


@dataclasses.dataclass
class _SectionTestConfig(ConfigClass):
    scheduler: Optional[_SimpleSchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    workers: List[_SimpleWorkerConfig] = dataclasses.field(default_factory=list, metadata=dict(section="workers"))


class TestSectionMetadata(unittest.TestCase):
    """Tests the section= metadata path in ConfigClass / _from_args."""

    def _build(self, toml_data):
        return _from_args(_SectionTestConfig, {}, toml_data)

    def test_single_table_populates_optional(self) -> None:
        toml = {"scheduler": {"host": "192.168.1.1", "port": 9999}}
        config = self._build(toml)
        self.assertIsNotNone(config.scheduler)
        self.assertEqual(config.scheduler.host, "192.168.1.1")
        self.assertEqual(config.scheduler.port, 9999)

    def test_absent_section_gives_none(self) -> None:
        config = self._build({})
        self.assertIsNone(config.scheduler)

    def test_absent_list_section_gives_empty_list(self) -> None:
        config = self._build({})
        self.assertEqual(config.workers, [])

    def test_single_dict_in_list_section_gives_one_element(self) -> None:
        toml = {"workers": {"workers": 4}}
        config = self._build(toml)
        self.assertEqual(len(config.workers), 1)
        self.assertEqual(config.workers[0].workers, 4)

    def test_array_of_tables_gives_multiple_elements(self) -> None:
        toml = {"workers": [{"workers": 2}, {"workers": 8}]}
        config = self._build(toml)
        self.assertEqual(len(config.workers), 2)
        self.assertEqual(config.workers[0].workers, 2)
        self.assertEqual(config.workers[1].workers, 8)

    def test_both_sections_populated(self) -> None:
        toml = {"scheduler": {"host": "10.0.0.1", "port": 1234}, "workers": [{"workers": 3}]}
        config = self._build(toml)
        self.assertIsNotNone(config.scheduler)
        self.assertEqual(config.scheduler.host, "10.0.0.1")
        self.assertEqual(len(config.workers), 1)
        self.assertEqual(config.workers[0].workers, 3)


# ---------------------------------------------------------------------------
# Minimal ScalerAllConfig-like stub for end-to-end tests
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _StubScalerAllConfig(ConfigClass):
    scheduler: Optional[_SimpleSchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    workers: List[_SimpleWorkerConfig] = dataclasses.field(default_factory=list, metadata=dict(section="workers"))


class TestScalerAllEndToEnd(unittest.TestCase):
    """End-to-end tests for the section= flow through ConfigClass.parse."""

    def _make_toml_data(self, data):
        """Return a mock _load_toml that yields the given data dict."""
        return patch("scaler.config.config_class._load_toml", return_value=data)

    @patch("sys.argv", ["scaler_all", "--config", "test.toml"])
    def test_no_recognized_sections_exits(self) -> None:
        with self._make_toml_data({}):
            config = _StubScalerAllConfig.parse("scaler_all", "all")
        self.assertIsNone(config.scheduler)
        self.assertEqual(config.workers, [])

    @patch("sys.argv", ["scaler_all", "--config", "test.toml"])
    def test_scheduler_section_populated(self) -> None:
        toml = {"scheduler": {"host": "127.0.0.1", "port": 8516}}
        with self._make_toml_data(toml):
            config = _StubScalerAllConfig.parse("scaler_all", "all")
        self.assertIsNotNone(config.scheduler)
        self.assertEqual(config.scheduler.host, "127.0.0.1")

    @patch("sys.argv", ["scaler_all", "--config", "test.toml"])
    def test_workers_list_populated(self) -> None:
        toml = {"workers": [{"workers": 4}, {"workers": 8}]}
        with self._make_toml_data(toml):
            config = _StubScalerAllConfig.parse("scaler_all", "all")
        self.assertEqual(len(config.workers), 2)

    @patch("sys.argv", ["scaler_all", "--help"])
    def test_help_exits(self) -> None:
        with self.assertRaises(SystemExit):
            _StubScalerAllConfig.parse("scaler_all", "all")


class TestAIOMain(unittest.TestCase):
    """Tests for scaler_all main() process spawning logic."""

    def test_no_sections_exits_with_code_1(self) -> None:
        from scaler.entry_points.all import main

        with patch("scaler.config.config_class._load_toml", return_value={}), patch(
            "sys.argv", ["scaler_all", "--config", "test.toml"]
        ):
            with self.assertRaises(SystemExit) as ctx:
                main()
        self.assertEqual(ctx.exception.code, 1)


# ---------------------------------------------------------------------------
# Tests for the real ScalerAllConfig shape
# ---------------------------------------------------------------------------


class TestScalerAllConfigShape(unittest.TestCase):
    """Tests that ScalerAllConfig reads the [logging] and [worker] sections."""

    def _parse(self, toml_data):
        from scaler.entry_points.all import ScalerAllConfig

        with patch("scaler.config.config_class._load_toml", return_value=toml_data), patch(
            "sys.argv", ["scaler_all", "--config", "test.toml"]
        ):
            return ScalerAllConfig.parse("scaler_all", "all")

    def test_logging_section_sets_level(self) -> None:
        config = self._parse({"logging": {"level": "DEBUG"}})
        self.assertEqual(config.logging.level, "DEBUG")

    def test_logging_section_absent_uses_defaults(self) -> None:
        from scaler.config.common.logging import LoggingConfig

        config = self._parse({})
        self.assertEqual(config.logging.level, LoggingConfig().level)
        self.assertEqual(config.logging.paths, LoggingConfig().paths)

    def test_worker_manager_native_parsed_from_toml(self) -> None:
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig

        toml = {
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
            }
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 1)
        self.assertIsInstance(config.worker_managers[0], NativeWorkerManagerConfig)

    def test_worker_config_io_threads_from_toml(self) -> None:
        toml = {
            "worker": {"io_threads": 4},
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
            },
        }
        config = self._parse(toml)
        self.assertEqual(config.worker.io_threads, 4)

    def test_worker_config_event_loop_from_toml(self) -> None:
        toml = {
            "worker": {"event_loop": "builtin"},
            "worker_manager": {
                "type": "baremetal_native",
                "scheduler_address": "tcp://127.0.0.1:6378",
                "worker_manager_id": "wm-1",
            },
        }
        config = self._parse(toml)
        self.assertEqual(config.worker.event_loop, "builtin")

    def test_multiple_worker_managers_from_toml(self) -> None:
        toml = {
            "worker_manager": [
                {"type": "baremetal_native", "scheduler_address": "tcp://127.0.0.1:6378", "worker_manager_id": "wm-1"},
                {"type": "baremetal_native", "scheduler_address": "tcp://127.0.0.1:6378", "worker_manager_id": "wm-2"},
            ]
        }
        config = self._parse(toml)
        self.assertEqual(len(config.worker_managers), 2)

    def test_worker_manager_inherits_scheduler_address(self) -> None:
        """Worker managers without scheduler_address inherit it from [scheduler]."""
        from scaler.config.types.zmq import ZMQConfig

        toml = {
            "scheduler": {"scheduler_address": "tcp://10.0.0.1:9999"},
            "worker_manager": {"type": "baremetal_native", "worker_manager_id": "wm-1"},
        }
        config = self._parse(toml)
        from scaler.entry_points.all import _resolve_addresses

        _resolve_addresses(config)
        wm_addr = config.worker_managers[0].worker_manager_config.scheduler_address
        self.assertEqual(wm_addr, ZMQConfig.from_string("tcp://10.0.0.1:9999"))

    def test_scheduler_address_chosen_when_omitted(self) -> None:
        """[scheduler] without scheduler_address gets a TCP port-0 address for OS-assigned binding."""
        from scaler.config.types.zmq import ZMQConfig, ZMQType
        from scaler.entry_points.all import _resolve_addresses

        toml = {"scheduler": {}, "worker_manager": {"type": "baremetal_native", "worker_manager_id": "wm-1"}}
        config = self._parse(toml)
        _resolve_addresses(config)
        addr = config.scheduler.scheduler_address
        self.assertIsInstance(addr, ZMQConfig)
        self.assertEqual(addr.type, ZMQType.tcp)
        self.assertEqual(addr.port, 0)

    def test_worker_manager_missing_address_without_scheduler_exits(self) -> None:
        """Worker manager without scheduler_address and no [scheduler] section exits with code 1."""
        from scaler.entry_points.all import _resolve_addresses

        toml = {"worker_manager": {"type": "baremetal_native", "worker_manager_id": "wm-1"}}
        config = self._parse(toml)
        with self.assertRaises(SystemExit) as ctx:
            _resolve_addresses(config)
        self.assertEqual(ctx.exception.code, 1)

    def test_object_storage_server_section_populates_field(self) -> None:
        """[object_storage_server] section in config populates ScalerAllConfig.object_storage_server."""
        from scaler.config.section.object_storage_server import ObjectStorageServerConfig
        from scaler.entry_points.all import ScalerAllConfig

        toml = {"object_storage_server": {"object_storage_address": "tcp://127.0.0.1:2345"}}
        with patch("scaler.config.config_class._load_toml", return_value=toml), patch(
            "sys.argv", ["scaler_all", "--config", "test.toml"]
        ):
            config = ScalerAllConfig.parse("scaler_all", "all")
        self.assertIsNotNone(config.object_storage_server)
        self.assertIsInstance(config.object_storage_server, ObjectStorageServerConfig)
        self.assertEqual(config.object_storage_server.object_storage_address.host, "127.0.0.1")
        self.assertEqual(config.object_storage_server.object_storage_address.port, 2345)

    def test_object_storage_server_section_empty_uses_defaults(self) -> None:
        """Empty [object_storage_server] section defaults to 0.0.0.0:0."""
        from scaler.entry_points.all import ScalerAllConfig

        toml: dict = {"object_storage_server": {}}
        with patch("scaler.config.config_class._load_toml", return_value=toml), patch(
            "sys.argv", ["scaler_all", "--config", "test.toml"]
        ):
            config = ScalerAllConfig.parse("scaler_all", "all")
        self.assertIsNotNone(config.object_storage_server)
        self.assertEqual(config.object_storage_server.object_storage_address.host, "0.0.0.0")
        self.assertEqual(config.object_storage_server.object_storage_address.port, 0)


class TestApplyActualAddresses(unittest.TestCase):
    """Tests for _apply_actual_addresses — propagation of actual bound ports to worker managers."""

    def _make_config(self, scheduler_address=None, object_storage_address=None):
        from scaler.config.common.worker_manager import WorkerManagerConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
        from scaler.entry_points.all import ScalerAllConfig

        wmc = WorkerManagerConfig(scheduler_address=scheduler_address, object_storage_address=object_storage_address)
        wm = NativeWorkerManagerConfig(worker_manager_config=wmc, worker_manager_id="wm-1")
        config = ScalerAllConfig.__new__(ScalerAllConfig)
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.common.worker import WorkerConfig

        config.logging = LoggingConfig()
        config.worker = WorkerConfig()
        config.scheduler = None
        config.object_storage_server = None
        config.worker_managers = [wm]
        return config

    def _actual_sched_addr(self, host="0.0.0.0", port=54321):
        from scaler.config.types.zmq import ZMQConfig

        return ZMQConfig.from_string(f"tcp://{host}:{port}")

    def _actual_os_addr(self, host="0.0.0.0", port=54322):
        from scaler.config.types.object_storage_server import ObjectStorageAddressConfig

        return ObjectStorageAddressConfig(host=host, port=port)

    # --- scheduler_address ---

    def test_scheduler_address_none_uses_loopback_and_actual_port(self) -> None:
        """omitted scheduler_address → 127.0.0.1 at actual bound port."""
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(scheduler_address=None)
        _apply_actual_addresses(config, self._actual_sched_addr(host="0.0.0.0", port=9001), None)
        addr = config.worker_managers[0].worker_manager_config.scheduler_address
        self.assertEqual(addr.host, "127.0.0.1")
        self.assertEqual(addr.port, 9001)

    def test_scheduler_address_port_zero_keeps_host(self) -> None:
        """scheduler_address with port=0 keeps configured host, fills in actual port."""
        from scaler.config.types.zmq import ZMQConfig
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(scheduler_address=ZMQConfig.from_string("tcp://10.0.1.50:0"))
        _apply_actual_addresses(config, self._actual_sched_addr(port=9001), None)
        addr = config.worker_managers[0].worker_manager_config.scheduler_address
        self.assertEqual(addr.host, "10.0.1.50")
        self.assertEqual(addr.port, 9001)

    def test_scheduler_address_explicit_port_unchanged(self) -> None:
        """scheduler_address with explicit non-zero port is left alone."""
        from scaler.config.types.zmq import ZMQConfig
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(scheduler_address=ZMQConfig.from_string("tcp://127.0.0.1:6378"))
        _apply_actual_addresses(config, self._actual_sched_addr(port=9001), None)
        addr = config.worker_managers[0].worker_manager_config.scheduler_address
        self.assertEqual(addr.host, "127.0.0.1")
        self.assertEqual(addr.port, 6378)

    def test_scheduler_address_none_no_scheduler_exits(self) -> None:
        """omitted scheduler_address with no actual_sched_addr → sys.exit(1)."""
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(scheduler_address=None)
        with self.assertRaises(SystemExit) as ctx:
            _apply_actual_addresses(config, actual_sched_addr=None, actual_os_addr=None)
        self.assertEqual(ctx.exception.code, 1)

    # --- object_storage_address ---

    def test_object_storage_address_none_uses_loopback_and_actual_port(self) -> None:
        """omitted object_storage_address → 127.0.0.1 at actual bound port."""
        from scaler.config.types.zmq import ZMQConfig
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(
            scheduler_address=ZMQConfig.from_string("tcp://127.0.0.1:6378"), object_storage_address=None
        )
        _apply_actual_addresses(config, None, self._actual_os_addr(host="0.0.0.0", port=6379))
        os_addr = config.worker_managers[0].worker_manager_config.object_storage_address
        self.assertEqual(os_addr.host, "127.0.0.1")
        self.assertEqual(os_addr.port, 6379)

    def test_object_storage_address_port_zero_keeps_host(self) -> None:
        """object_storage_address with port=0 keeps configured host, fills in actual port."""
        from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
        from scaler.config.types.zmq import ZMQConfig
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(
            scheduler_address=ZMQConfig.from_string("tcp://127.0.0.1:6378"),
            object_storage_address=ObjectStorageAddressConfig(host="10.0.1.50", port=0),
        )
        _apply_actual_addresses(config, None, self._actual_os_addr(host="0.0.0.0", port=6379))
        os_addr = config.worker_managers[0].worker_manager_config.object_storage_address
        self.assertEqual(os_addr.host, "10.0.1.50")
        self.assertEqual(os_addr.port, 6379)

    def test_object_storage_address_explicit_port_unchanged(self) -> None:
        """object_storage_address with explicit non-zero port is left alone."""
        from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
        from scaler.config.types.zmq import ZMQConfig
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(
            scheduler_address=ZMQConfig.from_string("tcp://127.0.0.1:6378"),
            object_storage_address=ObjectStorageAddressConfig(host="127.0.0.1", port=5555),
        )
        _apply_actual_addresses(config, None, self._actual_os_addr(port=6379))
        os_addr = config.worker_managers[0].worker_manager_config.object_storage_address
        self.assertEqual(os_addr.host, "127.0.0.1")
        self.assertEqual(os_addr.port, 5555)

    def test_object_storage_address_none_no_actual_os_addr_unchanged(self) -> None:
        """No object storage server running → object_storage_address stays None."""
        from scaler.config.types.zmq import ZMQConfig
        from scaler.entry_points.all import _apply_actual_addresses

        config = self._make_config(
            scheduler_address=ZMQConfig.from_string("tcp://127.0.0.1:6378"), object_storage_address=None
        )
        _apply_actual_addresses(config, None, actual_os_addr=None)
        self.assertIsNone(config.worker_managers[0].worker_manager_config.object_storage_address)


class TestRunWorkerManager(unittest.TestCase):
    """Tests that _run_worker_manager calls register_event_loop with worker_config.event_loop."""

    def _make_native_config(self):
        from scaler.config.common.worker_manager import WorkerManagerConfig
        from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
        from scaler.config.types.zmq import ZMQConfig

        wmc = WorkerManagerConfig(scheduler_address=ZMQConfig.from_string("tcp://localhost:6378"))
        return NativeWorkerManagerConfig(worker_manager_config=wmc, worker_manager_id="wm-test")

    def test_register_event_loop_called_with_worker_config_event_loop(self) -> None:
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.common.worker import WorkerConfig
        from scaler.entry_points.all import _run_worker_manager

        config = self._make_native_config()
        worker_config = WorkerConfig(event_loop="builtin")

        with patch("scaler.entry_points.all.register_event_loop") as mock_reg, patch(
            "scaler.entry_points.all.setup_logger"
        ), patch("scaler.worker_manager_adapter.baremetal.native.NativeWorkerManager") as mock_nm:
            mock_nm.return_value.run.return_value = None
            _run_worker_manager(LoggingConfig(), config, worker_config)

        mock_reg.assert_called_once_with("builtin")

    def test_setup_logger_called_with_logging_config(self) -> None:
        from scaler.config.common.logging import LoggingConfig
        from scaler.config.common.worker import WorkerConfig
        from scaler.entry_points.all import _run_worker_manager

        config = self._make_native_config()
        logging_cfg = LoggingConfig(level="WARNING")

        with patch("scaler.entry_points.all.setup_logger") as mock_log, patch(
            "scaler.entry_points.all.register_event_loop"
        ), patch("scaler.worker_manager_adapter.baremetal.native.NativeWorkerManager") as mock_nm:
            mock_nm.return_value.run.return_value = None
            _run_worker_manager(logging_cfg, config, WorkerConfig())

        mock_log.assert_called_once_with(logging_cfg.paths, logging_cfg.config_file, "WARNING")
