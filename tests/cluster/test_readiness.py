import multiprocessing
import time
import unittest

from scaler.cluster.readiness import wait_for_ready_report
from scaler.config.types.address import AddressConfig
from scaler.scheduler.scheduler import BoundSchedulerAddresses


class TestWaitForReadyReport(unittest.TestCase):
    def test_a_report_of_the_wrong_type_is_rejected(self) -> None:
        """A process reporting something other than the expected type fails the wait with both type names."""
        context = multiprocessing.get_context("spawn")
        ready_receiver, ready_sender = context.Pipe(duplex=False)
        ready_sender.send(True)

        process = context.Process(target=time.sleep, args=(0,))
        self.addCleanup(process.join)
        process.start()

        with self.assertRaisesRegex(TypeError, "Scheduler reported bool, expected BoundSchedulerAddresses"):
            wait_for_ready_report(
                ready_receiver,
                process,
                "Scheduler",
                AddressConfig.from_string("tcp://127.0.0.1:0"),
                BoundSchedulerAddresses,
            )
