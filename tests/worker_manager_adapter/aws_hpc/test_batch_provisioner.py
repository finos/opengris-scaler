import unittest
from unittest.mock import MagicMock

from scaler.worker_manager_adapter.aws_hpc.worker_manager import BatchWorkerProvisioner
from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS


def _make_provisioner(max_concurrent_jobs: int = 100) -> BatchWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.max_concurrent_jobs = max_concurrent_jobs
    return BatchWorkerProvisioner(config)


class TestBatchWorkerProvisionerShape(unittest.TestCase):
    """AWS Batch is a proxy provisioner: its unit is a local process, not a remote resource.

    The controller turns the desired task concurrency into a unit count; all this backend has to
    declare is how many task slots one of its units supplies.
    """

    def test_a_unit_supplies_the_concurrent_job_limit(self) -> None:
        self.assertEqual(_make_provisioner(max_concurrent_jobs=100).task_concurrency_per_unit(), 100)

    def test_the_limit_follows_configuration(self) -> None:
        self.assertEqual(_make_provisioner(max_concurrent_jobs=7).task_concurrency_per_unit(), 7)

    def test_the_number_of_proxy_processes_is_unbounded(self) -> None:
        self.assertEqual(_make_provisioner().max_units(), UNLIMITED_UNITS)

    def test_polling_a_local_process_is_cheap_enough_to_do_often(self) -> None:
        self.assertLessEqual(_make_provisioner().poll_interval_seconds(), 5.0)


if __name__ == "__main__":
    unittest.main()
