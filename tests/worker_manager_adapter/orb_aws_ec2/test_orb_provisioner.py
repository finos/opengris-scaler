import unittest
from unittest.mock import MagicMock

from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


def _make_provisioner(workers_per_instance: int = 1, max_instances: int = -1) -> ORBWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {"cpu": 4}
    return ORBWorkerProvisioner(
        config=config,
        max_instances=max_instances,
        sdk=MagicMock(),
        template_id="tmpl-123",
        workers_per_instance=workers_per_instance,
    )


class TestORBWorkerProvisionerShape(unittest.TestCase):
    """ORB EC2 is a nested provisioner: its unit is an instance running a child worker manager."""

    def test_a_unit_supplies_the_workers_of_its_instance(self) -> None:
        self.assertEqual(_make_provisioner(workers_per_instance=16).task_concurrency_per_unit(), 16)

    def test_max_units_is_the_configured_instance_cap(self) -> None:
        self.assertEqual(_make_provisioner(max_instances=5).max_units(), 5)

    def test_an_unlimited_cap_is_passed_through(self) -> None:
        self.assertEqual(_make_provisioner(max_instances=-1).max_units(), -1)


class TestORBWorkerProvisionerPoll(unittest.IsolatedAsyncioTestCase):
    async def test_it_reports_the_instances_it_created(self) -> None:
        provisioner = _make_provisioner()
        provisioner._instance_ids = {"i-abc", "i-def"}

        self.assertEqual(await provisioner.poll_units(), {"i-abc", "i-def"})

    async def test_destroying_an_unknown_instance_asks_orb_nothing(self) -> None:
        provisioner = _make_provisioner()

        await provisioner.destroy_unit("i-never-created")

        provisioner._sdk.create_return_request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
