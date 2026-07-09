"""End-to-end control-plane test: the AWS Batch worker-manager infrastructure against mocked AWS.

The ``aws_hpc`` backend offloads *task execution* to AWS Batch (each task becomes a Batch job); its
one-time infrastructure -- S3 bucket, IAM roles, EC2 compute environment, job queue, job definition --
is created by ``AWSBatchProvisioner.provision_all``. This test drives that real provisioning flow over
the real boto3 request path and asserts the resources actually exist in the mocked AWS backend.

Backend: moto (implements Batch in-process). Real task execution by a Batch container is out of scope for
a mock (needs real AWS + Docker); the container-scaling e2e covers "provisioned worker actually runs a
task".
"""

import unittest

from scaler.utility.logging.utility import setup_logger
from tests.integration import INTEGRATION_SKIP_REASON, RUN_INTEGRATION_TESTS
from tests.integration._aws_backend import MockedAWS
from tests.utility.utility import logging_test_name

PREFIX = "scaler-it-batch"


@unittest.skipUnless(RUN_INTEGRATION_TESTS, INTEGRATION_SKIP_REASON)
class TestBatchProvisioningControlPlane(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self._aws = MockedAWS().__enter__()
        self.addCleanup(self._aws.__exit__, None, None, None)
        self._aws.seed_batch_service_role()
        self.batch = self._aws.client("batch")
        self.s3 = self._aws.client("s3")

    def _provision(self):
        from scaler.worker_manager_adapter.aws_hpc.utility.provisioner import AWSBatchProvisioner

        provisioner = AWSBatchProvisioner(aws_region=self._aws.region, prefix=PREFIX)
        # instance_types=["optimal"] is the AWS Batch keyword (moto rejects the placeholder default).
        return provisioner.provision_all(
            container_image="opengris-scaler:it", vcpus=1, memory_mb=2048, max_vcpus=16, instance_types=["optimal"]
        )

    def test_provision_all_creates_batch_infrastructure(self) -> None:
        result = self._provision()

        # The compute environment, job queue and job definition are really registered in the backend.
        env_names = [
            ce["computeEnvironmentName"] for ce in self.batch.describe_compute_environments()["computeEnvironments"]
        ]
        queue_names = [q["jobQueueName"] for q in self.batch.describe_job_queues()["jobQueues"]]
        job_defs = self.batch.describe_job_definitions(status="ACTIVE")["jobDefinitions"]

        self.assertIn(f"{PREFIX}-compute", env_names)
        self.assertIn(result["job_queue_name"], queue_names)
        self.assertTrue(any(jd["jobDefinitionName"] == result["job_definition_name"] for jd in job_defs))

        # And the payload S3 bucket exists (head_bucket raises if it does not).
        self.s3.head_bucket(Bucket=result["s3_bucket"])

    def test_provisioning_is_idempotent(self) -> None:
        first = self._provision()
        # A second provision_all must reuse the same job queue (no duplicate) and not raise.
        second = self._provision()

        self.assertEqual(first["job_queue_name"], second["job_queue_name"])
        queue_names = [q["jobQueueName"] for q in self.batch.describe_job_queues()["jobQueues"]]
        self.assertEqual(queue_names.count(first["job_queue_name"]), 1)

        # The compute environment must not be duplicated either. (The job DEFINITION is intentionally
        # re-registered as a new revision each call -- keep_latest bounds it -- so its count is not asserted.)
        env_names = [
            ce["computeEnvironmentName"] for ce in self.batch.describe_compute_environments()["computeEnvironments"]
        ]
        self.assertEqual(env_names.count(f"{PREFIX}-compute"), 1)


if __name__ == "__main__":
    unittest.main()
