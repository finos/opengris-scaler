import unittest

from scaler.config import defaults
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.types.address import AddressConfig


def _make_config(max_task_concurrency: int) -> WorkerManagerConfig:
    return WorkerManagerConfig(
        scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:2345"),
        worker_manager_id="test-wm",
        max_task_concurrency=max_task_concurrency,
    )


class TestWorkerManagerConfig(unittest.TestCase):
    def test_zero_max_task_concurrency_is_accepted(self) -> None:
        self.assertEqual(_make_config(0).max_task_concurrency, 0)

    def test_upper_limit_max_task_concurrency_is_accepted(self) -> None:
        config = _make_config(defaults.MAX_TASK_CONCURRENCY_LIMIT)
        self.assertEqual(config.max_task_concurrency, defaults.MAX_TASK_CONCURRENCY_LIMIT)

    def test_negative_max_task_concurrency_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _make_config(-1)

    def test_above_upper_limit_max_task_concurrency_is_rejected(self) -> None:
        with self.assertRaises(ValueError):
            _make_config(defaults.MAX_TASK_CONCURRENCY_LIMIT + 1)
