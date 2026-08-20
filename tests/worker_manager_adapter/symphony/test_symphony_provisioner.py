from __future__ import annotations

import unittest
from unittest.mock import MagicMock

try:
    from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerProvisioner
    from scaler.worker_manager_adapter.unit_provisioner import UNLIMITED_UNITS

    _SYMPHONY_AVAILABLE = True
except ImportError:
    _SYMPHONY_AVAILABLE = False


def _make_provisioner(max_task_concurrency: int = -1) -> SymphonyWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.worker_manager_config.max_task_concurrency = max_task_concurrency
    config.worker_manager_config.worker_manager_id = "test-wm"
    config.service_name = "test-service"
    return SymphonyWorkerProvisioner(config)


@unittest.skipUnless(_SYMPHONY_AVAILABLE, "soamapi not installed")
class TestSymphonyWorkerProvisionerShape(unittest.TestCase):
    """Symphony is a proxy provisioner: its unit is a local process, not a remote resource."""

    def test_a_unit_supplies_the_configured_concurrency(self) -> None:
        self.assertEqual(_make_provisioner(max_task_concurrency=4).task_concurrency_per_unit(), 4)

    def test_an_unlimited_concurrency_still_supplies_at_least_one_slot(self) -> None:
        # ceil(desired / per_unit) in the controller must never divide by zero or by a negative.
        self.assertGreaterEqual(_make_provisioner(max_task_concurrency=-1).task_concurrency_per_unit(), 1)

    def test_the_number_of_proxy_processes_is_unbounded(self) -> None:
        self.assertEqual(_make_provisioner().max_units(), UNLIMITED_UNITS)


if __name__ == "__main__":
    unittest.main()
