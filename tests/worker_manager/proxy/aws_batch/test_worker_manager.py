import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.config.section.aws_hpc_worker_manager import AWSHPCBackend
from scaler.worker_manager.proxy.aws_batch.worker_manager import AWSBatchWorkerManager, AWSBatchWorkerProvisioner


def _make_provisioner(max_concurrent_jobs: int = 100) -> AWSBatchWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.max_concurrent_jobs = max_concurrent_jobs
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    return AWSBatchWorkerProvisioner(config, max_units=-1)


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [MagicMock(key=k, value=v) for k, v in capabilities.items()]
    return request


class TestAWSBatchWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_converts_task_concurrency_to_process_count(self) -> None:
        provisioner = _make_provisioner(max_concurrent_jobs=100)
        request = _make_request(task_concurrency=150, capabilities={})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 2)  # ceil(150 / 100) = 2

    async def test_task_concurrency_zero_sets_desired_to_zero(self) -> None:
        provisioner = _make_provisioner()
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([_make_request(task_concurrency=100, capabilities={})])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 1)
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([_make_request(task_concurrency=0, capabilities={})])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 0)


class TestAWSBatchWorkerManagerMaxTaskConcurrency(unittest.TestCase):
    def _run(self, max_task_concurrency: int) -> MagicMock:
        config = MagicMock()
        config.backend = AWSHPCBackend.batch
        config.max_concurrent_jobs = 100
        config.worker_config.per_worker_capabilities.capabilities = {}
        config.worker_manager_config.max_task_concurrency = max_task_concurrency
        config.worker_manager_config.scale_down_cooldown_seconds = 0
        with patch("scaler.worker_manager.proxy.aws_batch.worker_manager.WorkerManagerRunner") as runner:
            AWSBatchWorkerManager(config).run()
        return runner

    def test_max_task_concurrency_caps_units(self) -> None:
        """max_task_concurrency=250 with 100 jobs per unit caps the manager at ceil(250 / 100) = 3 units."""
        runner = self._run(max_task_concurrency=250)
        self.assertEqual(runner.call_args.kwargs["max_provisioner_units"], 3)
        self.assertEqual(runner.call_args.kwargs["worker_provisioner"]._capacity_coordinator._max_unit_count, 3)

    def test_unlimited_max_task_concurrency_leaves_units_uncapped(self) -> None:
        runner = self._run(max_task_concurrency=-1)
        self.assertEqual(runner.call_args.kwargs["max_provisioner_units"], -1)
        self.assertEqual(runner.call_args.kwargs["worker_provisioner"]._capacity_coordinator._max_unit_count, -1)
