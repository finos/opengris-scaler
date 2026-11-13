import unittest
from unittest.mock import MagicMock, patch

from scaler.worker_manager.cloud.oci.worker_manager import OCIInstancesWorkerProvisioner


class TestOCIInstancesWorkerProvisioner(unittest.TestCase):
    def test_startup_timeout_comes_from_the_config(self) -> None:
        """A container instance that installs scaler from source may take longer than the default to start."""
        config = MagicMock()
        config.cloud_children_config.unit_startup_timeout_seconds = 1800
        with patch.object(OCIInstancesWorkerProvisioner, "_initialize_oci_client"):
            provisioner = OCIInstancesWorkerProvisioner(config, max_instances=-1)
        self.assertEqual(provisioner.startup_timeout_seconds(), 1800)
