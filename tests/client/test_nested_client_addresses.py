"""Which object storage address a client opened inside a worker uses.

It takes its worker's address only when it takes its worker's scheduler too.
"""

import unittest
from typing import Optional

from scaler.client.client import Client
from scaler.config.types.address import AddressConfig
from scaler.worker.agent.processor.processor import _current_processor


def resolve(scheduler_address: Optional[str], object_storage_address: Optional[str]) -> Optional[str]:
    """`Client.__resolve_object_storage_address`, whose name Python mangles."""
    return Client._Client__resolve_object_storage_address(  # type: ignore[attr-defined]
        scheduler_address, object_storage_address
    )


class _FakeProcessor:
    """Only what the address resolution reads."""

    def __init__(self, storage: str) -> None:
        self._storage = AddressConfig.from_string(storage)

    def object_storage_address(self) -> AddressConfig:
        return self._storage


class TestNestedClientAddresses(unittest.TestCase):
    def setUp(self) -> None:
        self._token = _current_processor.set(_FakeProcessor("tcp://storage.inside:6379"))  # type: ignore[arg-type]
        self.addCleanup(_current_processor.reset, self._token)

    def test_a_client_taking_its_worker_scheduler_takes_its_worker_storage(self) -> None:
        self.assertEqual(resolve(None, None), "tcp://storage.inside:6379")

    def test_a_given_storage_address_still_wins(self) -> None:
        self.assertEqual(resolve(None, "tcp://elsewhere:1234"), "tcp://elsewhere:1234")

    def test_a_given_scheduler_address_leaves_storage_to_that_scheduler(self) -> None:
        """A client connecting to another cluster uses the address that cluster advertises."""
        self.assertIsNone(resolve("tcp://other-scheduler:6378", None))


class TestClientOutsideAWorker(unittest.TestCase):
    def test_nothing_is_resolved_so_the_scheduler_is_asked(self) -> None:
        """Outside a worker there is nothing to inherit, and None means the advertised address."""
        self.assertIsNone(resolve(None, None))


if __name__ == "__main__":
    unittest.main()
