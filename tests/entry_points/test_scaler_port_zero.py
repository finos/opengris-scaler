import unittest
from typing import Any, Dict
from unittest.mock import patch

from scaler.config.types.address import AddressConfig
from scaler.entry_points.scaler import ScalerAllConfig, _port_zero_errors, _resolve

BOUND = AddressConfig.from_string("tcp://0.0.0.0:45605")


def _parse(data: Dict[str, Any]) -> ScalerAllConfig:
    with (
        patch("sys.argv", ["scaler", "scaler_all.toml"]),
        patch("scaler.config.config_class._load_toml", return_value=data),
    ):
        return ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)


class TestResolvePortZero(unittest.TestCase):
    def test_port_zero_takes_the_bound_port_and_keeps_its_host(self) -> None:
        """A connecting address naming port 0 gets the bound port, and its host stays as written."""
        resolved = _resolve(AddressConfig.from_string("tcp://10.0.0.5:0"), BOUND)
        self.assertEqual(resolved, AddressConfig.from_string("tcp://10.0.0.5:45605"))

    def test_a_named_port_is_left_alone(self) -> None:
        """Only port 0 is resolved: a config naming real ports behaves as before."""
        address = AddressConfig.from_string("tcp://0.0.0.0:8517")
        self.assertEqual(_resolve(address, BOUND), address)


class TestPortZeroErrors(unittest.TestCase):
    def test_port_zero_with_no_section_to_bind_it_is_rejected(self) -> None:
        """A connecting field naming port 0 needs a section in the same config that binds it."""
        config = _parse(
            {"scheduler": {"bind_address": "tcp://127.0.0.1:0", "object_storage_address": "tcp://127.0.0.1:0"}}
        )
        self.assertEqual(
            _port_zero_errors(config),
            ["scheduler.object_storage_address names port 0, but no section in this config binds it"],
        )

    def test_a_wildcard_connecting_host_is_rejected(self) -> None:
        """0.0.0.0 cannot be connected to, so a connecting field naming it with port 0 is rejected."""
        config = _parse(
            {
                "object_storage_server": {"bind_address": "tcp://0.0.0.0:0"},
                "scheduler": {"bind_address": "tcp://0.0.0.0:0", "object_storage_address": "tcp://0.0.0.0:0"},
            }
        )
        self.assertEqual(
            _port_zero_errors(config),
            [
                "scheduler.object_storage_address names 0.0.0.0, which cannot be connected to: "
                "name 127.0.0.1 or another host"
            ],
        )

    def test_port_zero_with_its_section_present_is_accepted(self) -> None:
        """A wildcard bind is fine when the connecting field names a host that can be connected to."""
        config = _parse(
            {
                "object_storage_server": {"bind_address": "tcp://0.0.0.0:0"},
                "scheduler": {"bind_address": "tcp://0.0.0.0:0", "object_storage_address": "tcp://127.0.0.1:0"},
            }
        )
        self.assertEqual(_port_zero_errors(config), [])
