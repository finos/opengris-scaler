import os
import tempfile
import unittest
from typing import Optional

from scaler.config.common.cloud_children import CloudChildrenConfig
from scaler.config.types.address import AddressConfig
from scaler.worker_manager.cloud.child_command import child_link_arguments, load_requirements_content


class TestLoadRequirementsContent(unittest.TestCase):
    def test_returns_literal_string(self):
        content = load_requirements_content("opengris-scaler>=1.0\nboto3\n")
        self.assertEqual(content, "opengris-scaler>=1.0\nboto3\n")

    def test_reads_valid_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("opengris-scaler\n")
            path = f.name
        try:
            content = load_requirements_content(path)
            self.assertEqual(content, "opengris-scaler\n")
        finally:
            os.unlink(path)

    def test_returns_literal_string_when_path_does_not_exist(self):
        content = load_requirements_content("/nonexistent/requirements.txt")
        self.assertEqual(content, "/nonexistent/requirements.txt")


class TestChildLinkArguments(unittest.TestCase):
    BOUND_ADDRESS = "tcp://172.31.8.78:8518"
    ADVERTISED_ADDRESS = "tcp://34.228.81.179:8518"

    def _arguments(self, advertised_children_address: Optional[str]) -> str:
        cloud_children_config = CloudChildrenConfig(
            advertised_children_address=(
                AddressConfig.from_string(advertised_children_address) if advertised_children_address else None
            )
        )
        return child_link_arguments(AddressConfig.from_string(self.BOUND_ADDRESS), cloud_children_config, "unit-1", 4)

    def test_units_dial_advertised_children_address(self):
        """Units behind NAT dial the advertised address, not the address the manager binds."""
        arguments = self._arguments(self.ADVERTISED_ADDRESS)
        self.assertIn(f"--parent-address {self.ADVERTISED_ADDRESS} ", arguments)

    def test_units_dial_children_address_without_advertised_address(self):
        """Without an advertised address, units dial the address the manager binds."""
        arguments = self._arguments(None)
        self.assertIn(f"--parent-address {self.BOUND_ADDRESS} ", arguments)

    def test_an_advertised_address_naming_port_0_takes_the_bound_port(self):
        """Behind 1:1 NAT, an advertised address naming port 0 takes the port the manager bound."""
        arguments = self._arguments("tcp://34.228.81.179:0")
        self.assertIn("--parent-address tcp://34.228.81.179:8518 ", arguments)
