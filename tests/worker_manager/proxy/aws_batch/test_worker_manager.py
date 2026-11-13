import unittest
from unittest.mock import MagicMock, patch

from scaler.config.section.aws_hpc_worker_manager import AWSHPCBackend
from scaler.worker_manager.proxy.aws_batch.worker_manager import AWSBatchWorkerManager, AWSBatchWorkerProvisioner


class TestAWSBatchWorkerProvisioner(unittest.TestCase):
    def test_one_unit_supplies_the_concurrent_job_limit(self) -> None:
        config = MagicMock()
        config.max_concurrent_jobs = 100
        provisioner = AWSBatchWorkerProvisioner(config, max_units=-1, children_address=MagicMock())
        self.assertEqual(provisioner.task_concurrency_per_unit(), 100)
        self.assertEqual(provisioner.max_units(), -1)


class TestAWSBatchWorkerManagerMaxTaskConcurrency(unittest.TestCase):
    def _run(self, max_task_concurrency: int) -> MagicMock:
        config = MagicMock()
        config.backend = AWSHPCBackend.batch
        config.max_concurrent_jobs = 100
        config.worker_config.per_worker_capabilities.capabilities = {}
        config.worker_manager_config.max_task_concurrency = max_task_concurrency
        with patch("scaler.worker_manager.proxy.aws_batch.worker_manager.WorkerManagerRunner") as runner:
            AWSBatchWorkerManager(config).run()
        return runner

    def test_max_task_concurrency_caps_units(self) -> None:
        """max_task_concurrency=250 with 100 jobs per unit caps the manager at ceil(250 / 100) = 3 units."""
        runner = self._run(max_task_concurrency=250)
        self.assertEqual(runner.call_args.kwargs["provisioner"].max_units(), 3)

    def test_unlimited_max_task_concurrency_leaves_units_uncapped(self) -> None:
        runner = self._run(max_task_concurrency=-1)
        self.assertEqual(runner.call_args.kwargs["provisioner"].max_units(), -1)
