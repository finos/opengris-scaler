from __future__ import annotations

import unittest
from unittest.mock import MagicMock

try:
    from scaler.worker_manager.proxy.symphony.worker_manager import SymphonyWorkerProvisioner

    _SYMPHONY_AVAILABLE = True
except ImportError:
    _SYMPHONY_AVAILABLE = False


@unittest.skipUnless(_SYMPHONY_AVAILABLE, "soamapi not installed")
class TestSymphonyWorkerProvisioner(unittest.TestCase):
    def test_one_unit_counts_as_one_task_slot(self) -> None:
        config = MagicMock()
        config.worker_manager_config.max_task_concurrency = 4
        config.worker_manager_config.worker_manager_id = "test-wm"
        provisioner = SymphonyWorkerProvisioner(config, MagicMock())
        self.assertEqual(provisioner.task_concurrency_per_unit(), 1)
        self.assertEqual(provisioner.max_units(), 4)
