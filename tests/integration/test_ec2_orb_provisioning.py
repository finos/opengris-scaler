"""End-to-end control-plane test: the ORB (EC2) worker manager provisioning against moto-mocked AWS.

It drives the real ``ORBWorkerProvisioner`` through the real ``orb-py`` SDK, whose provisioning path ends
in boto3 ``ec2.run_instances`` / ``terminate_instances`` (moto intercepts the SDK's boto3 EC2 client), and
asserts on the mocked EC2 instance state.

NOTE: this covers the *control plane* (does the manager launch/track/terminate instances correctly). moto
does not boot the instance's user-data, so instances never connect back as workers; real task execution is
covered by tests/scheduler/test_scaling.py.
"""

import sys
import unittest
import uuid
from unittest.mock import MagicMock, patch

from scaler.utility.logging.utility import setup_logger
from tests.integration import INTEGRATION_SKIP_REASON, RUN_INTEGRATION_TESTS
from tests.integration._aws_backend import MockedAWS
from tests.integration._harness import async_wait_until, desired_requests
from tests.utility.utility import logging_test_name


# orb-py imports typing.assert_never, which only exists on Python 3.11+, so its SDK cannot load on 3.10.
@unittest.skipUnless(RUN_INTEGRATION_TESTS, INTEGRATION_SKIP_REASON)
@unittest.skipUnless(sys.version_info >= (3, 11), "orb-py requires Python 3.11+")
class TestORBEC2ProvisioningControlPlane(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        import os
        import tempfile

        from orb.config.managers.configuration_manager import ConfigurationManager
        from orb.infrastructure.di.container import get_container
        from orb.sdk.client import ORBClient

        setup_logger()
        logging_test_name(self)

        # ORB persists config/templates/work/logs (via ORB_ROOT_DIR) plus a cwd-relative metrics/ dir.
        # Redirect both into a temp dir so nothing is written into the repo. chdir is safe here because
        # the test is fully in-process (no scaler subprocesses depend on the cwd).
        self._storage_dir = tempfile.TemporaryDirectory(prefix="scaler-orb-it-")
        self.addCleanup(self._storage_dir.cleanup)
        root_patch = patch.dict(os.environ, {"ORB_ROOT_DIR": self._storage_dir.name})
        root_patch.start()
        self.addCleanup(root_patch.stop)
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(self._storage_dir.name)

        self._aws = MockedAWS().__enter__()
        self.addCleanup(self._aws.__exit__, None, None, None)
        self.env = self._aws.seed_ec2_environment()
        self.ec2 = self._aws.client("ec2")

        # Make the ORB provisioner's post-request instance polling fast (defaults to 5s per poll).
        interval_patch = patch(
            "scaler.worker_manager_adapter.orb_aws_ec2.worker_manager.ORB_AWS_EC2_POLLING_INTERVAL_SECONDS", 0.1
        )
        interval_patch.start()
        self.addCleanup(interval_patch.stop)

        # Bring up the real ORB SDK the same way ORBAWSEC2WorkerManager._run does, then register a
        # RunInstances template pointing at the seeded VPC resources.
        app_config = {
            "provider": {
                "selection_policy": "FIRST_AVAILABLE",
                "providers": [
                    {
                        "name": "aws-default",
                        "type": "aws",
                        "enabled": True,
                        "priority": 1,
                        "config": {"region": self.env.region, "profile": None},
                    }
                ],
            },
            "storage": {"type": "json"},
        }
        os.environ["AWS_DEFAULT_REGION"] = self.env.region
        get_container().register_instance(ConfigurationManager, ConfigurationManager(config_dict=app_config))

        self.sdk = await ORBClient(app_config=app_config).__aenter__()
        self.addAsyncCleanup(self.sdk.__aexit__, None, None, None)
        # ORB's json storage persists templates across tests (and runs), so use a unique id per test.
        self.template_id = f"scaler-it-orb-{uuid.uuid4().hex[:12]}"
        self.addAsyncCleanup(self.sdk.delete_template, template_id=self.template_id)
        await self.sdk.create_template(
            template_id=self.template_id,
            name="scaler-it-orb",
            image_id=self.env.image_id,
            provider_api="RunInstances",
            instance_type="t2.micro",
            max_instances=10,
            provider_name="aws-default",
            machine_types={"t2.micro": 1},
            subnet_ids=list(self.env.subnets),
            security_group_ids=list(self.env.security_groups),
            key_name=self.env.key_name,
            user_data="#!/bin/bash\ntrue\n",
            tags={},
        )
        await self.sdk.validate_template(template_id=self.template_id)

    def _running_count(self, instance_ids) -> int:
        # Assert against the specific instance IDs the provisioner tracks, not a global count, so the
        # assertion stays precise regardless of any other instances seeded in the backend.
        instance_ids = list(instance_ids)
        if not instance_ids:
            return 0
        reservations = self.ec2.describe_instances(
            InstanceIds=instance_ids, Filters=[{"Name": "instance-state-name", "Values": ["running", "pending"]}]
        )["Reservations"]
        return sum(len(reservation["Instances"]) for reservation in reservations)

    def _make_provisioner(self, workers_per_instance: int = 1, max_instances: int = -1):
        from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBWorkerProvisioner

        config = MagicMock()
        config.worker_config.per_worker_capabilities.capabilities = {}
        provisioner = ORBWorkerProvisioner(
            config=config,
            max_instances=max_instances,
            sdk=self.sdk,
            template_id=self.template_id,
            workers_per_instance=workers_per_instance,
        )
        self.addAsyncCleanup(provisioner.terminate)
        return provisioner

    async def test_scale_up_launches_ec2_instances(self) -> None:
        provisioner = self._make_provisioner(workers_per_instance=1)

        await provisioner.set_desired_task_concurrency(desired_requests(2))
        await async_wait_until(lambda: provisioner.active_unit_count() == 2, message="scale-up to 2 instances")

        self.assertEqual(self._running_count(provisioner._units), 2)

    async def test_workers_per_instance_reduces_instance_count(self) -> None:
        # 8 desired workers / 4 workers-per-instance = 2 EC2 instances.
        provisioner = self._make_provisioner(workers_per_instance=4)

        await provisioner.set_desired_task_concurrency(desired_requests(8))
        await async_wait_until(lambda: provisioner.active_unit_count() == 2, message="scale-up to 2 instances")

        self.assertEqual(self._running_count(provisioner._units), 2)

    async def test_terminate_stops_all_instances(self) -> None:
        provisioner = self._make_provisioner(workers_per_instance=1)
        await provisioner.set_desired_task_concurrency(desired_requests(3))
        await async_wait_until(lambda: provisioner.active_unit_count() == 3, message="scale-up to 3 instances")
        instance_ids = list(provisioner._units)
        self.assertEqual(self._running_count(instance_ids), 3)

        await provisioner.terminate()
        self.assertEqual(provisioner.active_unit_count(), 0)
        await async_wait_until(lambda: self._running_count(instance_ids) == 0, message="all instances terminated")


if __name__ == "__main__":
    unittest.main()
