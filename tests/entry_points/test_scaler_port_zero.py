import unittest
from typing import Any, Dict
from unittest.mock import patch

from scaler.config.types.address import AddressConfig
from scaler.entry_points.scaler import ScalerAllConfig, _port_zero_without_target, _resolve

BOUND = AddressConfig.from_string("tcp://0.0.0.0:45605")


def _parse(data: Dict[str, Any]) -> ScalerAllConfig:
    with (
        patch("sys.argv", ["scaler", "scaler_all.toml"]),
        patch("scaler.config.config_class._load_toml", return_value=data),
    ):
        return ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)


class TestResolvePortZero(unittest.TestCase):
    def test_port_zero_takes_the_bound_port_and_keeps_its_host(self) -> None:
        """A connecting address naming port 0 gets the bound port and keeps the host it names."""
        resolved = _resolve(AddressConfig.from_string("tcp://10.0.0.5:0"), BOUND)
        self.assertEqual(resolved, AddressConfig.from_string("tcp://10.0.0.5:45605"))

    def test_a_wildcard_host_connects_over_loopback(self) -> None:
        """0.0.0.0 cannot be connected to, so a connecting address naming it reaches the server over loopback."""
        resolved = _resolve(AddressConfig.from_string("tcp://0.0.0.0:0"), BOUND)
        self.assertEqual(resolved, AddressConfig.from_string("tcp://127.0.0.1:45605"))

    def test_a_named_port_is_left_alone(self) -> None:
        """Only port 0 is resolved: a config naming real ports behaves as before."""
        address = AddressConfig.from_string("tcp://0.0.0.0:8517")
        self.assertEqual(_resolve(address, BOUND), address)


class TestPortZeroWithoutTarget(unittest.TestCase):
    def test_port_zero_with_no_section_to_bind_it_is_reported(self) -> None:
        """A connecting field naming port 0 needs a section in the same config that binds it."""
        config = _parse(
            {"scheduler": {"bind_address": "tcp://127.0.0.1:0", "object_storage_address": "tcp://127.0.0.1:0"}}
        )
        self.assertEqual(_port_zero_without_target(config), ["scheduler.object_storage_address"])

    def test_port_zero_with_its_section_present_is_accepted(self) -> None:
        config = _parse(
            {
                "object_storage_server": {"bind_address": "tcp://127.0.0.1:0"},
                "scheduler": {"bind_address": "tcp://127.0.0.1:0", "object_storage_address": "tcp://127.0.0.1:0"},
            }
        )
        self.assertEqual(_port_zero_without_target(config), [])
