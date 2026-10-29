import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.worker_manager.cloud.orb_aws_ec2.worker_manager import ORBWorkerProvisioner


def _make_provisioner(workers_per_instance: int = 1, max_instances: int = -1) -> ORBWorkerProvisioner:
    config = MagicMock()
    config.worker_config.per_worker_capabilities.capabilities = {"cpu": 4}
    config.worker_manager_config.scale_down_cooldown_seconds = 0
    sdk = MagicMock()
    sdk.create_request = AsyncMock(return_value={"created_request_id": "req-001"})
    sdk.get_request_status = AsyncMock()
    sdk.cancel_request = AsyncMock(return_value={"request_id": "req-001", "status": "cancelled"})
    return ORBWorkerProvisioner(
        config=config,
        max_instances=max_instances,
        sdk=sdk,
        template_id="tmpl-123",
        workers_per_instance=workers_per_instance,
    )


def _make_request(task_concurrency: int, capabilities: dict) -> MagicMock:
    request = MagicMock()
    request.taskConcurrency = task_concurrency
    request.capabilities = [SimpleNamespace(name=k, value=v) for k, v in capabilities.items()]
    return request


class TestORBWorkerProvisionerConcurrencyConversion(unittest.IsolatedAsyncioTestCase):
    async def test_converts_workers_to_instance_count(self) -> None:
        provisioner = _make_provisioner(workers_per_instance=16)
        request = _make_request(task_concurrency=100, capabilities={"cpu": 4})
        with patch.object(provisioner._capacity_coordinator, "_reconcile", new_callable=AsyncMock):
            await provisioner.set_desired_task_concurrency([request])
        self.assertEqual(provisioner._capacity_coordinator._desired_unit_count, 7)  # ceil(100 / 16) = 7


class TestORBWorkerProvisionerStartUnitsCancellation(unittest.IsolatedAsyncioTestCase):
    async def test_start_units_success_does_not_cancel(self) -> None:
        provisioner = _make_provisioner()
        provisioner._sdk.get_request_status.return_value = {
            "requests": [{"status": "complete", "machine_ids": ["i-1", "i-2"]}]
        }
        with patch("scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.asyncio.sleep", new_callable=AsyncMock):
            await provisioner.start_units(2)

        self.assertEqual(provisioner._units, ["i-1", "i-2"])
        provisioner._sdk.cancel_request.assert_not_called()

    async def test_terminal_failure_statuses_cancel_request_immediately(self) -> None:
        for terminal_status in ("failed", "cancelled", "partial", "timeout"):
            with self.subTest(status=terminal_status):
                provisioner = _make_provisioner()
                provisioner._sdk.get_request_status.return_value = {
                    "requests": [{"status": terminal_status, "machine_ids": ["i-1"]}]
                }
                with patch(
                    "scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.asyncio.sleep", new_callable=AsyncMock
                ) as mock_sleep:
                    with self.assertRaises(RuntimeError):
                        await provisioner.start_units(2)

                self.assertEqual(mock_sleep.await_count, 1)
                self.assertEqual(provisioner._units, [])
                provisioner._sdk.cancel_request.assert_awaited_once_with(request_id="req-001")

    async def test_polling_timeout_cancels_request(self) -> None:
        provisioner = _make_provisioner()
        provisioner._sdk.get_request_status.return_value = {
            "requests": [{"status": "in_progress", "machine_ids": ["i-1"]}]
        }
        with (
            patch("scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.ORB_AWS_EC2_MAX_POLLING_ATTEMPTS", 2),
            patch("scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.asyncio.sleep", new_callable=AsyncMock),
        ):
            with self.assertRaises(TimeoutError):
                await provisioner.start_units(2)

        self.assertEqual(provisioner._units, [])
        provisioner._sdk.cancel_request.assert_awaited_once_with(request_id="req-001")

    async def test_sdk_status_exception_cancels_request(self) -> None:
        provisioner = _make_provisioner()
        provisioner._sdk.get_request_status.side_effect = RuntimeError("poll transport error")
        with patch("scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaisesRegex(RuntimeError, "poll transport error"):
                await provisioner.start_units(1)

        provisioner._sdk.cancel_request.assert_awaited_once_with(request_id="req-001")

    async def test_cancelled_error_cancels_request(self) -> None:
        provisioner = _make_provisioner()
        provisioner._sdk.get_request_status.side_effect = asyncio.CancelledError()
        with patch("scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(asyncio.CancelledError):
                await provisioner.start_units(1)

        provisioner._sdk.cancel_request.assert_awaited_once_with(request_id="req-001")

    async def test_missing_request_id_does_not_cancel(self) -> None:
        provisioner = _make_provisioner()
        provisioner._sdk.create_request.return_value = {}
        with self.assertRaises(RuntimeError):
            await provisioner.start_units(1)

        provisioner._sdk.cancel_request.assert_not_called()

    async def test_logs_error_when_cancel_reports_failed_return(self) -> None:
        for cancel_payload in (
            {
                "request_id": "req-001",
                "status": "cancelled",
                "return_status": "failed",
                "return_message": "Return failed: EC2 error",
            },
            {
                "request_id": "req-001",
                "status": "cancelled",
                "requests": [
                    {
                        "request_id": "req-001",
                        "status": "cancelled",
                        "return_status": "failed",
                        "return_message": "Return failed: EC2 error",
                    }
                ],
            },
        ):
            with self.subTest(payload=cancel_payload):
                provisioner = _make_provisioner()
                provisioner._sdk.get_request_status.return_value = {
                    "requests": [{"status": "failed", "machine_ids": []}]
                }
                provisioner._sdk.cancel_request.return_value = cancel_payload

                with (
                    patch(
                        "scaler.worker_manager.cloud.orb_aws_ec2.worker_manager.asyncio.sleep", new_callable=AsyncMock
                    ),
                    self.assertLogs(
                        "scaler.worker_manager.cloud.orb_aws_ec2.worker_manager", level="ERROR"
                    ) as captured_logs,
                ):
                    with self.assertRaises(RuntimeError):
                        await provisioner.start_units(1)

                self.assertTrue(any("req-001" in msg for msg in captured_logs.output))
