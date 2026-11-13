import unittest
from unittest.mock import MagicMock, patch

from scaler.worker_manager.proxy.oci.worker_manager import OCIJobsWorkerManager


class TestOCIJobsWorkerManagerMaxTaskConcurrency(unittest.TestCase):
    def _run(self, max_task_concurrency: int) -> MagicMock:
        config = MagicMock()
        config.base_concurrency = 100
        config.worker_config.per_worker_capabilities.capabilities = {}
        config.worker_manager_config.max_task_concurrency = max_task_concurrency
        with patch("scaler.worker_manager.proxy.oci.worker_manager.WorkerManagerRunner") as runner:
            OCIJobsWorkerManager(config).run()
        return runner

    def test_max_task_concurrency_caps_units(self) -> None:
        """max_task_concurrency=250 with 100 jobs per unit caps the manager at ceil(250 / 100) = 3 units."""
        runner = self._run(max_task_concurrency=250)
        self.assertEqual(runner.call_args.kwargs["provisioner"].max_units(), 3)

    def test_unlimited_max_task_concurrency_leaves_units_uncapped(self) -> None:
        runner = self._run(max_task_concurrency=-1)
        self.assertEqual(runner.call_args.kwargs["provisioner"].max_units(), -1)
