"""End-to-end control-plane test: the REAL ECS worker manager provisioning against mocked AWS.

This exercises the actual boto3 request path (``ECSWorkerProvisioner`` -> AWS ECS API) rather
than a MagicMock, using the same ``build_set_desired_command`` object the scheduler emits over
the wire. It verifies the full scale-up / scale-down / terminate lifecycle by inspecting the
moto-mocked ECS backend's real task state.

NOTE: moto does not boot the provisioned containers, so these tasks never connect back as workers --
this test covers the *control plane* (does the manager call AWS correctly and track lifecycle). Real ECS
task execution is covered by the floci-backed e2e (``test_ecs_scaling_e2e.py``).
"""

import unittest

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from tests.integration import INTEGRATION_SKIP_REASON, RUN_INTEGRATION_TESTS
from tests.integration._aws_backend import MockedAWS, SeededECSEnvironment
from tests.integration._harness import async_wait_until, desired_requests
from tests.utility.utility import logging_test_name


def _make_ecs_config(
    env: SeededECSEnvironment, max_task_concurrency: int, ecs_task_cpu: int = 4
) -> ECSWorkerManagerConfig:
    return ECSWorkerManagerConfig(
        worker_manager_config=WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:65500"),
            worker_manager_id="wm-ecs-it",
            object_storage_address=AddressConfig.from_string("tcp://127.0.0.1:65501"),
            max_task_concurrency=max_task_concurrency,
        ),
        worker_config=WorkerConfig(per_worker_capabilities=WorkerCapabilities({})),
        logging_config=LoggingConfig(),
        aws_region=env.region,
        ecs_subnets=list(env.subnets),
        ecs_cluster=env.cluster,
        ecs_task_definition=env.task_definition,
        ecs_task_cpu=ecs_task_cpu,
    )


@unittest.skipUnless(RUN_INTEGRATION_TESTS, INTEGRATION_SKIP_REASON)
class TestECSProvisioningControlPlane(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self._aws = MockedAWS().__enter__()
        self.addCleanup(self._aws.__exit__, None, None, None)
        self.env = self._aws.seed_ecs_environment()
        self.ecs = self._aws.client("ecs")

    def _running_task_count(self) -> int:
        return len(self.ecs.list_tasks(cluster=self.env.cluster, desiredStatus="RUNNING")["taskArns"])

    def _make_provisioner(self, max_task_concurrency: int = -1, ecs_task_cpu: int = 4):
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerProvisioner

        provisioner = ECSWorkerProvisioner(_make_ecs_config(self.env, max_task_concurrency, ecs_task_cpu))
        self.addAsyncCleanup(provisioner.terminate)
        return provisioner

    async def test_construct_discovers_seeded_cluster_and_task_definition(self) -> None:
        provisioner = self._make_provisioner()
        # Adoption (not re-registration) means the family still has only the seeded revision, and the
        # provisioner points at it -- a substring check alone would also pass on a re-registered :2.
        revisions = self.ecs.list_task_definitions(familyPrefix=self.env.task_definition)["taskDefinitionArns"]
        self.assertEqual(len(revisions), 1)
        self.assertIn(self.env.task_definition, provisioner._ecs_task_definition)
        self.assertEqual(provisioner.active_unit_count(), 0)

    async def test_scale_up_launches_ecs_tasks(self) -> None:
        provisioner = self._make_provisioner(ecs_task_cpu=4)

        # ceil(8 / 4 cpu-per-task) = 2 ECS tasks.
        await provisioner.set_desired_task_concurrency(desired_requests(8))
        await async_wait_until(lambda: provisioner.active_unit_count() == 2, message="scale-up to 2 units")

        self.assertEqual(self._running_task_count(), 2)

    async def test_scale_down_stops_ecs_tasks(self) -> None:
        provisioner = self._make_provisioner(ecs_task_cpu=4)

        await provisioner.set_desired_task_concurrency(desired_requests(12))  # ceil(12/4) = 3
        await async_wait_until(lambda: provisioner.active_unit_count() == 3, message="scale-up to 3 units")
        self.assertEqual(self._running_task_count(), 3)

        await provisioner.set_desired_task_concurrency(desired_requests(4))  # ceil(4/4) = 1
        await async_wait_until(lambda: provisioner.active_unit_count() == 1, message="scale-down to 1 unit")
        self.assertEqual(self._running_task_count(), 1)

    async def test_max_task_concurrency_caps_scale_up(self) -> None:
        # max_task_concurrency=8 -> max_instances = ceil(8/4) = 2, even though we ask for more.
        provisioner = self._make_provisioner(max_task_concurrency=8, ecs_task_cpu=4)

        await provisioner.set_desired_task_concurrency(desired_requests(40))  # would want ceil(40/4)=10
        await async_wait_until(lambda: provisioner.active_unit_count() == 2, message="scale-up capped at 2 units")

        # Cap held at 2 (a broken cap would over-provision to ~10 and already have timed out above).
        self.assertEqual(provisioner.active_unit_count(), 2)
        self.assertEqual(self._running_task_count(), 2)

    async def test_terminate_stops_all_tasks(self) -> None:
        # Go through _make_provisioner so terminate is also registered as async cleanup -- otherwise a
        # timeout before the explicit terminate() below leaks the CapacityCoordinator reconcile task.
        provisioner = self._make_provisioner(ecs_task_cpu=4)
        await provisioner.set_desired_task_concurrency(desired_requests(8))
        await async_wait_until(lambda: provisioner.active_unit_count() == 2, message="scale-up to 2 units")

        await provisioner.terminate()
        self.assertEqual(provisioner.active_unit_count(), 0)
        self.assertEqual(self._running_task_count(), 0)


if __name__ == "__main__":
    unittest.main()
