import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager.cloud.oci.worker_manager import OCIInstancesWorkerProvisioner


class TestOCIInstancesWorkerProvisioner(unittest.TestCase):
    def test_startup_timeout_comes_from_the_config(self) -> None:
        """A container instance that installs scaler from source may take longer than the default to start."""
        config = MagicMock()
        config.cloud_children_config.unit_startup_timeout_seconds = 1800
        config.worker_config.telemetry_relay = False
        with patch.object(OCIInstancesWorkerProvisioner, "_initialize_oci_client"):
            provisioner = OCIInstancesWorkerProvisioner(config, max_instances=-1, children_address=MagicMock())
        self.assertEqual(provisioner.startup_timeout_seconds(), 1800)


class TestOCIInstancesWorkerProvisionerCreate(unittest.IsolatedAsyncioTestCase):
    async def test_create_returns_the_instance_without_waiting_for_it_to_become_active(self) -> None:
        """A handle from the first API call lets terminate destroy an instance that is still booting."""
        config = MagicMock()
        config.worker_config.per_worker_capabilities.capabilities = {}
        config.python_worker_environment.requirements_txt = ""
        config.worker_config.telemetry_relay = False
        config.container_instance_config.image_pull_username = None
        with patch.object(OCIInstancesWorkerProvisioner, "_initialize_oci_client"):
            provisioner = OCIInstancesWorkerProvisioner(config, max_instances=-1, children_address=MagicMock())
        client = MagicMock()
        client.create_container_instance.return_value.data.id = "ocid1.containerinstance.test"
        client.get_container_instance.return_value.data.lifecycle_state = "CREATING"
        provisioner._container_instances_client = client

        with patch("scaler.worker_manager.cloud.oci.worker_manager.asyncio.sleep", new_callable=AsyncMock):
            handle = await provisioner.create_unit("unit-1")

        self.assertEqual(handle, "ocid1.containerinstance.test")
        client.get_container_instance.assert_not_called()
