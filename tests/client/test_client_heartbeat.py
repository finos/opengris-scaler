import unittest

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

SCHEDULER_CLIENT_TIMEOUT_SECONDS = 2

LARGE_RESULT_SIZE_BYTES = 750_000_000


def noop(value: int) -> int:
    return value


def large_result(size: int) -> bytes:
    return b"1" * size


class TestClientHeartbeat(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.combo = SchedulerClusterCombo(
            n_workers=1, event_loop="builtin", client_timeout_seconds=SCHEDULER_CLIENT_TIMEOUT_SECONDS
        )
        self.address = self.combo.get_address()

    def tearDown(self) -> None:
        self.combo.shutdown()

    def test_client_non_blocking(self):
        """Receiving a large task result must not prevent the client agent's event loop to send heart-beats."""

        FUTURE_RESULT_TIMEOUT_SECONDS = 60

        with Client(self.address, heartbeat_interval_seconds=1) as client:
            future = client.submit(large_result, LARGE_RESULT_SIZE_BYTES)

            self.assertEqual(len(future.result(timeout=FUTURE_RESULT_TIMEOUT_SECONDS)), LARGE_RESULT_SIZE_BYTES)

            # the client must still be connected to the scheduler, i.e. it kept exchanging heartbeats while fetching the
            # result object.
            self.assertEqual(client.submit(noop, 1).result(timeout=FUTURE_RESULT_TIMEOUT_SECONDS), 1)


if __name__ == "__main__":
    unittest.main()
