import unittest
from unittest.mock import AsyncMock, MagicMock

from scaler.config import defaults
from scaler.protocol.capnp import WorkerManagerHeartbeat
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner


def _make_runner(
    max_provisioner_units: int, workers_per_provisioner_unit: int
) -> tuple[WorkerManagerRunner, AsyncMock]:
    runner = WorkerManagerRunner(
        address=MagicMock(),
        name="test_worker_manager",
        heartbeat_interval_seconds=1,
        capabilities={},
        max_provisioner_units=max_provisioner_units,
        worker_manager_id=b"test-wm",
        worker_provisioner=MagicMock(),
        workers_per_provisioner_unit=workers_per_provisioner_unit,
    )
    connector = AsyncMock()
    runner._connector_external = connector
    return runner, connector


class TestWorkerManagerRunnerHeartbeat(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_reports_product_of_units_and_workers_per_unit(self) -> None:
        runner, connector = _make_runner(max_provisioner_units=8, workers_per_provisioner_unit=4)
        await runner._send_heartbeat()
        heartbeat = connector.send.call_args.args[0]
        self.assertEqual(heartbeat.maxTaskConcurrency, 32)

    async def test_heartbeat_clamps_max_task_concurrency_to_limit_and_serializes(self) -> None:
        # Regression test: managers without a practical limit (e.g. AWS/OCI HPC) advertise
        # MAX_TASK_CONCURRENCY_LIMIT; the product with workers_per_provisioner_unit must be clamped so
        # the heartbeat still fits the wire format's UInt32.
        runner, connector = _make_runner(
            max_provisioner_units=defaults.MAX_TASK_CONCURRENCY_LIMIT, workers_per_provisioner_unit=100
        )
        await runner._send_heartbeat()
        heartbeat = connector.send.call_args.args[0]
        self.assertEqual(heartbeat.maxTaskConcurrency, defaults.MAX_TASK_CONCURRENCY_LIMIT)

        round_tripped = WorkerManagerHeartbeat.from_bytes(heartbeat.to_bytes())
        self.assertEqual(round_tripped.maxTaskConcurrency, defaults.MAX_TASK_CONCURRENCY_LIMIT)
