import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager.cloud.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


def _make_provisioner(sdk: MagicMock, workers_per_instance: int = 1, max_instances: int = -1) -> ORBWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {"cpu": 4}
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    return ORBWorkerProvisioner(
        config=config,
        max_instances=max_instances,
        sdk=sdk,
        template_kwargs={"image_id": "ami-1"},
        user_data_for_unit=lambda unit_id: f"user data of {unit_id}",
        workers_per_instance=workers_per_instance,
    )


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [SimpleNamespace(name=k, value=v) for k, v in capabilities.items()]
    return request


class TestORBWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_converts_workers_to_instance_count(self) -> None:
        provisioner = _make_provisioner(MagicMock(), workers_per_instance=16)
        request = _make_request(task_concurrency=100, capabilities={"cpu": 4})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 7)  # ceil(100 / 16) = 7


class TestORBWorkerProvisionerUnits(unittest.IsolatedAsyncioTestCase):
    async def test_each_unit_launches_one_instance_from_a_template_of_its_own(self) -> None:
        sdk = MagicMock()
        sdk.create_template = AsyncMock()
        sdk.validate_template = AsyncMock()
        sdk.create_request = AsyncMock(return_value={"created_request_id": "req-1"})
        sdk.get_request_status = AsyncMock(return_value={"requests": [{"status": "complete", "machine_ids": ["i-1"]}]})
        provisioner = _make_provisioner(sdk)

        with patch("asyncio.sleep", new_callable=AsyncMock), patch("uuid.uuid4", return_value=MagicMock(hex="unit-1")):
            await provisioner.start_units(1)

        self.assertEqual(provisioner._units, [("opengris-orb-unit-1", "i-1")])
        self.assertEqual(sdk.create_template.call_args.kwargs["user_data"], "user data of unit-1")
        sdk.create_request.assert_awaited_once_with(template_id="opengris-orb-unit-1", count=1)
