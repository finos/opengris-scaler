import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager.nested.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


class TestORBWorkerProvisioner(unittest.IsolatedAsyncioTestCase):
    async def test_create_unit_launches_one_instance_from_a_template_of_its_own(self) -> None:
        sdk = MagicMock()
        sdk.create_template = AsyncMock()
        sdk.validate_template = AsyncMock()
        sdk.create_request = AsyncMock(return_value={"created_request_id": "req-1"})
        sdk.get_request_status = AsyncMock(return_value={"requests": [{"status": "complete", "machine_ids": ["i-1"]}]})
        provisioner = ORBWorkerProvisioner(
            max_instances=-1,
            sdk=sdk,
            template_kwargs={"image_id": "ami-1"},
            user_data_for_unit=lambda unit_id: f"user data of {unit_id}",
            workers_per_instance=16,
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            self.assertEqual(await provisioner.create_unit("unit-1"), ("opengris-orb-unit-1", "i-1"))
        self.assertEqual(sdk.create_template.call_args.kwargs["user_data"], "user data of unit-1")
        sdk.create_request.assert_awaited_once_with(template_id="opengris-orb-unit-1", count=1)
        self.assertEqual(provisioner.task_concurrency_per_unit(), 16)
