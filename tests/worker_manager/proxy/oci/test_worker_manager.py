import unittest
from unittest.mock import MagicMock, patch

from scaler.config.types.address import AddressConfig
from scaler.worker_manager.proxy.oci.worker_manager import OCIJobsWorkerManager, OCIJobsWorkerProvisioner


def _provisioner(runner: MagicMock) -> OCIJobsWorkerProvisioner:
    """The provisioner the manager's runner creates once it binds its children address."""
    return runner.call_args.kwargs["create_worker_provisioner"](AddressConfig.from_string("tcp://127.0.0.1:2"))


class TestOCIJobsWorkerManagerMaxTaskConcurrency(unittest.TestCase):
    def _run(self, max_task_concurrency: int) -> MagicMock:
        config = MagicMock()
        config.base_concurrency = 100
        config.worker_config.per_worker_capabilities.capabilities = {}
        config.worker_manager_config.max_task_concurrency = max_task_concurrency
        config.worker_manager_config.scale_down_cooldown_seconds = 0
        with patch("scaler.worker_manager.proxy.oci.worker_manager.WorkerManagerRunner") as runner:
            OCIJobsWorkerManager(config).run()
        return runner

    def test_max_task_concurrency_caps_units(self) -> None:
        """max_task_concurrency=250 with 100 jobs per unit caps the manager at ceil(250 / 100) = 3 units."""
        runner = self._run(max_task_concurrency=250)
        self.assertEqual(runner.call_args.kwargs["max_provisioner_units"], 3)
        self.assertEqual(_provisioner(runner)._capacity_coordinator._max_unit_count, 3)

    def test_unlimited_max_task_concurrency_leaves_units_uncapped(self) -> None:
        runner = self._run(max_task_concurrency=-1)
        self.assertEqual(runner.call_args.kwargs["max_provisioner_units"], -1)
        self.assertEqual(_provisioner(runner)._capacity_coordinator._max_unit_count, -1)
