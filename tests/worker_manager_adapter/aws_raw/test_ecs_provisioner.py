import math
import unittest
from unittest.mock import MagicMock, patch

from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerProvisioner


def _make_provisioner(max_task_concurrency: int = -1, ecs_task_cpu: int = 4) -> ECSWorkerProvisioner:
    max_instances = math.ceil(max_task_concurrency / ecs_task_cpu) if max_task_concurrency != -1 else -1
    with patch("boto3.Session"):
        provisioner = ECSWorkerProvisioner.__new__(ECSWorkerProvisioner)
        provisioner._capabilities = {}
        provisioner._ecs_task_cpu = ecs_task_cpu
        provisioner._max_task_concurrency = max_task_concurrency
        provisioner._max_instances = max_instances
        provisioner._task_arns = set()
        provisioner._ecs_client = MagicMock()
        provisioner._ecs_cluster = "test-cluster"
        provisioner._ecs_task_definition = "test-td"
        provisioner._ecs_subnets = ["subnet-123"]
    return provisioner


class TestECSWorkerProvisionerShape(unittest.TestCase):
    """ECS is a nested provisioner: its unit is a task running a child worker manager."""

    def test_a_unit_supplies_one_task_slot_per_cpu(self) -> None:
        self.assertEqual(_make_provisioner(ecs_task_cpu=4).task_concurrency_per_unit(), 4)

    def test_max_units_is_derived_from_max_task_concurrency(self) -> None:
        self.assertEqual(_make_provisioner(max_task_concurrency=8, ecs_task_cpu=4).max_units(), 2)

    def test_an_unlimited_concurrency_leaves_the_unit_count_unbounded(self) -> None:
        self.assertEqual(_make_provisioner(max_task_concurrency=-1).max_units(), -1)

    def test_describing_tasks_is_charged_so_the_poll_is_slow(self) -> None:
        self.assertGreaterEqual(_make_provisioner().poll_interval_seconds(), 5.0)


class TestECSWorkerProvisionerPoll(unittest.IsolatedAsyncioTestCase):
    async def test_a_stopped_task_drops_out_of_the_poll(self) -> None:
        provisioner = _make_provisioner()
        provisioner._task_arns = {"arn-running", "arn-stopped"}
        provisioner._ecs_client.describe_tasks.return_value = {
            "tasks": [
                {"taskArn": "arn-running", "lastStatus": "RUNNING"},
                {"taskArn": "arn-stopped", "lastStatus": "STOPPED"},
            ]
        }

        alive = await provisioner.poll_units()

        self.assertEqual(alive, {"arn-running"})
        self.assertEqual(provisioner._task_arns, {"arn-running"})

    async def test_polling_with_no_tasks_asks_ecs_nothing(self) -> None:
        provisioner = _make_provisioner()

        self.assertEqual(await provisioner.poll_units(), set())
        provisioner._ecs_client.describe_tasks.assert_not_called()


if __name__ == "__main__":
    unittest.main()
