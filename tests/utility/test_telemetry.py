import os
import time
import unittest
from typing import Callable
from unittest import mock

from scaler import Client, SchedulerClusterCombo
from scaler.utility.logging.utility import setup_logger
from tests.utility.otlp_receiver import OTLPReceiver
from tests.utility.utility import logging_test_name

TELEMETRY_TIMEOUT_SECONDS = 30
POLL_SECONDS = 0.1

# export within a fraction of a second, so a test waits on the cluster, not on the SDK's default 5s and 60s intervals
FAST_EXPORT_ENVIRONMENT = {
    "OTEL_METRIC_EXPORT_INTERVAL": "200",
    "OTEL_BSP_SCHEDULE_DELAY": "100",
    "OTEL_BLRP_SCHEDULE_DELAY": "100",
}


def wait_for(condition: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + TELEMETRY_TIMEOUT_SECONDS
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {description}")
        time.sleep(POLL_SECONDS)


class TestTelemetry(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.receiver = OTLPReceiver()
        self.receiver.start()
        self.addCleanup(self.receiver.stop)

        # spawned cluster processes read the environment when they start
        environment = mock.patch.dict(
            os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": self.receiver.endpoint, **FAST_EXPORT_ENVIRONMENT}
        )
        environment.start()
        self.addCleanup(environment.stop)

        self.cluster = SchedulerClusterCombo(n_workers=1)
        self.addCleanup(self.cluster.shutdown)

    def test_scheduler_exports_cluster_metrics(self) -> None:
        """The scheduler exports task, worker, and event loop metrics to the OTLP endpoint."""
        with Client(address=self.cluster.get_address()) as client:
            self.assertEqual(client.submit(pow, 2, 3).result(), 8)

        expected = {"scaler.scheduler.tasks", "scaler.scheduler.workers", "scaler.scheduler.event_loop.lag"}
        wait_for(lambda: expected <= set(self.receiver.metric_names), f"metrics {expected}")

    def test_scheduler_logs_worker_connect_as_event(self) -> None:
        """A worker connecting is a log record with the worker and worker manager IDs as attributes."""
        wait_for(lambda: bool(self.receiver.logs_with_event("scaler.worker.connected")), "worker connected event")

        event = self.receiver.logs_with_event("scaler.worker.connected")[0]
        self.assertEqual(event.service_name, "scaler-scheduler")
        self.assertTrue(str(event.attributes["scaler.worker.id"]).startswith("Worker|"))
        self.assertIn("scaler.worker_manager.id", event.attributes)
