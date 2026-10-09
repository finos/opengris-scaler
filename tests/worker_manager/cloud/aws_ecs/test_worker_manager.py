import unittest
from unittest.mock import MagicMock, patch

from scaler.worker_manager.cloud.aws_ecs.worker_manager import ECSWorkerProvisioner


def _make_provisioner(max_task_concurrency: int = -1, ecs_task_cpu: int = 4) -> ECSWorkerProvisioner:
    config = MagicMock()
    config.worker_manager_config.max_task_concurrency = max_task_concurrency
    config.ecs_task_cpu = ecs_task_cpu
    with patch("boto3.Session"):
        return ECSWorkerProvisioner(config)


class TestECSWorkerProvisioner(unittest.IsolatedAsyncioTestCase):
    def test_one_unit_supplies_the_task_cpu_count(self) -> None:
        provisioner = _make_provisioner(max_task_concurrency=10, ecs_task_cpu=4)
        self.assertEqual(provisioner.task_concurrency_per_unit(), 4)
        self.assertEqual(provisioner.max_units(), 3)  # ceil(10 / 4)

    def test_no_limit_stays_unlimited(self) -> None:
        self.assertEqual(_make_provisioner(max_task_concurrency=-1).max_units(), -1)
