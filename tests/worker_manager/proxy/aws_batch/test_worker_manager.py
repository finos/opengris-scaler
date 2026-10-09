import unittest
from unittest.mock import MagicMock

from scaler.worker_manager.proxy.aws_batch.worker_manager import AWSBatchWorkerProvisioner


class TestAWSBatchWorkerProvisioner(unittest.TestCase):
    def test_one_unit_supplies_the_concurrent_job_limit(self) -> None:
        config = MagicMock()
        config.max_concurrent_jobs = 100
        provisioner = AWSBatchWorkerProvisioner(config, MagicMock())
        self.assertEqual(provisioner.task_concurrency_per_unit(), 100)
        self.assertEqual(provisioner.max_units(), -1)
