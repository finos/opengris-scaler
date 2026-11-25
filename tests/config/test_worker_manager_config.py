import unittest

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import DRAIN_TIMEOUT_NONE
from scaler.config.types.address import AddressConfig
from scaler.utility.identifiers import WorkerManagerID


def _config() -> WorkerManagerConfig:
    return WorkerManagerConfig(
        scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:1"),
        worker_manager_id=WorkerManagerID.from_string("m"),
    )


class TestDrainTimeout(unittest.TestCase):
    def test_a_drain_has_no_deadline_by_default(self):
        self.assertEqual(_config().drain_timeout_seconds, DRAIN_TIMEOUT_NONE)

    def test_a_value_below_no_deadline_is_refused(self):
        with self.assertRaises(ValueError):
            WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:1"),
                worker_manager_id=WorkerManagerID.from_string("m"),
                drain_timeout_seconds=DRAIN_TIMEOUT_NONE - 1,
            )
