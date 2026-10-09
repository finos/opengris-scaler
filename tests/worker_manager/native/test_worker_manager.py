import unittest
from unittest.mock import MagicMock, patch

from scaler.config.types.address import AddressConfig
from scaler.worker_manager.native.worker_manager import NativeWorkerProvisioner


def _make_provisioner(max_task_concurrency: int = -1) -> NativeWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.worker_manager_config.max_task_concurrency = max_task_concurrency
    config.worker_manager_config.worker_manager_id = "test-wm"
    config.worker_type = "NAT"
    return NativeWorkerProvisioner(config, AddressConfig.from_string("tcp://127.0.0.1:2"))


class TestNativeWorkerProvisioner(unittest.IsolatedAsyncioTestCase):
    def test_one_unit_is_one_worker(self) -> None:
        provisioner = _make_provisioner(max_task_concurrency=4)
        self.assertEqual(provisioner.task_concurrency_per_unit(), 1)
        self.assertEqual(provisioner.max_units(), 4)

    async def test_create_unit_names_the_worker_after_the_unit(self) -> None:
        provisioner = _make_provisioner()
        with patch("scaler.worker_manager.native.worker_manager.Worker") as worker_class:
            handle = await provisioner.create_unit("unit-1")
        self.assertIs(handle, worker_class.return_value)
        self.assertEqual(worker_class.call_args.kwargs["name"], "NAT|unit-1")
        worker_class.return_value.start.assert_called_once()
