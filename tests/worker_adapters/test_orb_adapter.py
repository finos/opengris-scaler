"""Tests for the ORB Worker Adapter.

These tests verify the dummy adapter and the real ORBWorkerAdapter interface
without requiring a running Kubernetes cluster or ORB processes.

Test classes:
- TestDummyORBConfig: Configuration dataclass validation
- TestDummyORBWorkerAdapter: Core adapter functionality (start/shutdown/status)
- TestDummyORBWorkerAdapterWebhook: HTTP webhook endpoint handling
- TestORBWorkerAdapterInterface: Real adapter with mocked ORB API calls
"""

import asyncio
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop

from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupNotFoundError
from scaler.worker_adapter.orb_adapter.dummy_adapter import DummyORBConfig, DummyORBWorkerAdapter


class TestDummyORBConfig(unittest.TestCase):
    """Tests for DummyORBConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values are set correctly."""
        config = DummyORBConfig()

        self.assertEqual(config.adapter_web_host, "127.0.0.1")
        self.assertEqual(config.adapter_web_port, 8080)
        self.assertEqual(config.max_workers, 10)
        self.assertEqual(config.workers_per_group, 1)
        self.assertEqual(config.template_id, "dummy-template")

    def test_custom_config(self):
        """Test custom configuration values are preserved."""
        config = DummyORBConfig(
            adapter_web_host="0.0.0.0",
            adapter_web_port=9090,
            max_workers=20,
            workers_per_group=4,
            template_id="custom-template",
        )

        self.assertEqual(config.adapter_web_host, "0.0.0.0")
        self.assertEqual(config.adapter_web_port, 9090)
        self.assertEqual(config.max_workers, 20)
        self.assertEqual(config.workers_per_group, 4)
        self.assertEqual(config.template_id, "custom-template")


class TestDummyORBWorkerAdapter(unittest.TestCase):
    """Tests for DummyORBWorkerAdapter core functionality.

    These tests verify the adapter's worker lifecycle management:
    - Starting worker groups
    - Shutting down worker groups
    - Capacity enforcement
    - Status reporting
    """

    def setUp(self):
        """Set up test fixtures with single and multi-worker configurations."""
        self.config = DummyORBConfig(max_workers=5, workers_per_group=1)
        self.adapter = DummyORBWorkerAdapter(self.config)

        self.config_multi = DummyORBConfig(max_workers=10, workers_per_group=3)
        self.adapter_multi = DummyORBWorkerAdapter(self.config_multi)

    def _run_async(self, coro):
        """Helper to run async coroutines in synchronous tests."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def test_start_worker_group_returns_valid_id(self):
        """Test that starting a worker group returns a properly formatted ID."""
        worker_group_id = self._run_async(self.adapter.start_worker_group())

        self.assertIsNotNone(worker_group_id)
        self.assertIsInstance(worker_group_id, bytes)
        self.assertTrue(worker_group_id.startswith(b"orb-dummy-"))

    def test_start_worker_group_tracks_group(self):
        """Test that started groups are tracked in internal state."""
        worker_group_id = self._run_async(self.adapter.start_worker_group())

        self.assertIn(worker_group_id, self.adapter._worker_groups)
        group_info = self.adapter._worker_groups[worker_group_id]
        self.assertEqual(group_info.worker_count, 1)
        self.assertEqual(len(group_info.worker_names), 1)
        self.assertEqual(group_info.status, "running")

    def test_start_multiple_worker_groups_unique_ids(self):
        """Test that each worker group gets a unique ID."""
        group1 = self._run_async(self.adapter.start_worker_group())
        group2 = self._run_async(self.adapter.start_worker_group())
        group3 = self._run_async(self.adapter.start_worker_group())

        self.assertEqual(len(self.adapter._worker_groups), 3)
        self.assertNotEqual(group1, group2)
        self.assertNotEqual(group2, group3)
        self.assertNotEqual(group1, group3)

    def test_start_worker_group_capacity_exceeded_single_worker(self):
        """Test CapacityExceededError when max_workers limit reached (1 worker/group)."""
        # Fill capacity: 5 groups × 1 worker = 5 workers (max)
        for _ in range(5):
            self._run_async(self.adapter.start_worker_group())

        with self.assertRaises(CapacityExceededError) as ctx:
            self._run_async(self.adapter.start_worker_group())

        self.assertIn("5", str(ctx.exception))  # Should mention max workers

    def test_start_worker_group_capacity_exceeded_multi_worker(self):
        """Test CapacityExceededError with multiple workers per group."""
        # max_workers=10, workers_per_group=3
        # 3 groups × 3 workers = 9 workers (under limit)
        # 4th group would be 12 workers (exceeds limit)
        self._run_async(self.adapter_multi.start_worker_group())  # 3 workers
        self._run_async(self.adapter_multi.start_worker_group())  # 6 workers
        self._run_async(self.adapter_multi.start_worker_group())  # 9 workers

        with self.assertRaises(CapacityExceededError):
            self._run_async(self.adapter_multi.start_worker_group())

    def test_shutdown_worker_group_removes_from_tracking(self):
        """Test that shutdown removes the group from internal tracking."""
        worker_group_id = self._run_async(self.adapter.start_worker_group())
        self.assertIn(worker_group_id, self.adapter._worker_groups)

        self._run_async(self.adapter.shutdown_worker_group(worker_group_id))

        self.assertNotIn(worker_group_id, self.adapter._worker_groups)

    def test_shutdown_worker_group_frees_capacity(self):
        """Test that shutdown frees capacity for new workers."""
        # Fill capacity
        groups = []
        for _ in range(5):
            groups.append(self._run_async(self.adapter.start_worker_group()))

        # Shutdown one group
        self._run_async(self.adapter.shutdown_worker_group(groups[0]))

        # Should be able to start a new group now
        new_group = self._run_async(self.adapter.start_worker_group())
        self.assertIsNotNone(new_group)

    def test_shutdown_nonexistent_worker_group(self):
        """Test that shutting down a non-existent group raises WorkerGroupNotFoundError."""
        with self.assertRaises(WorkerGroupNotFoundError) as ctx:
            self._run_async(self.adapter.shutdown_worker_group(b"orb-nonexistent-group"))

        self.assertIn("nonexistent", str(ctx.exception))

    def test_get_worker_group_status_returns_orb_format(self):
        """Test that status response matches ORB API format."""
        worker_group_id = self._run_async(self.adapter.start_worker_group())

        status = self._run_async(self.adapter.get_worker_group_status(worker_group_id))

        # Verify ORB-compatible structure
        self.assertIsNotNone(status)
        self.assertIn("requests", status)
        self.assertEqual(len(status["requests"]), 1)

        request_status = status["requests"][0]
        self.assertIn("requestId", request_status)
        self.assertEqual(request_status["status"], "complete")
        self.assertIn("machines", request_status)

    def test_get_worker_group_status_machine_info(self):
        """Test that status includes correct machine information."""
        worker_group_id = self._run_async(self.adapter.start_worker_group())
        status = self._run_async(self.adapter.get_worker_group_status(worker_group_id))

        machines = status["requests"][0]["machines"]
        self.assertEqual(len(machines), 1)

        machine = machines[0]
        self.assertEqual(machine["status"], "running")
        self.assertEqual(machine["result"], "succeed")
        self.assertIn("machineId", machine)
        self.assertIn("name", machine)

    def test_get_worker_group_status_not_found(self):
        """Test that status for non-existent group returns None."""
        status = self._run_async(self.adapter.get_worker_group_status(b"orb-nonexistent"))
        self.assertIsNone(status)

    def test_worker_names_sequential_format(self):
        """Test that worker names follow sequential naming convention."""
        worker_group_id = self._run_async(self.adapter_multi.start_worker_group())
        group_info = self.adapter_multi._worker_groups[worker_group_id]

        # Should have 3 workers with names ending in -0, -1, -2
        self.assertEqual(len(group_info.worker_names), 3)
        for i, name in enumerate(group_info.worker_names):
            self.assertTrue(name.endswith(f"-{i}"), f"Worker name '{name}' should end with '-{i}'")

    def test_unlimited_workers_mode(self):
        """Test adapter with max_workers=-1 allows unlimited workers."""
        config = DummyORBConfig(max_workers=-1, workers_per_group=1)
        adapter = DummyORBWorkerAdapter(config)

        # Should be able to create many groups without error
        for _ in range(100):
            self._run_async(adapter.start_worker_group())

        self.assertEqual(len(adapter._worker_groups), 100)

    def test_create_app_returns_aiohttp_application(self):
        """Test that create_app returns a valid aiohttp Application."""
        app = self.adapter.create_app()

        self.assertIsNotNone(app)
        # Verify webhook route is registered
        routes = [r.resource.canonical for r in app.router.routes()]
        self.assertIn("/", routes)


class TestDummyORBWorkerAdapterWebhook(AioHTTPTestCase):
    """Tests for the DummyORBWorkerAdapter webhook HTTP endpoint.

    These tests verify the REST API contract that the Scaler scheduler
    uses to communicate with worker adapters.
    """

    async def get_application(self):
        """Create the aiohttp application for testing."""
        config = DummyORBConfig(max_workers=5, workers_per_group=2)
        self.adapter = DummyORBWorkerAdapter(config)
        return self.adapter.create_app()

    @unittest_run_loop
    async def test_get_worker_adapter_info_response(self):
        """Test get_worker_adapter_info returns correct adapter metadata."""
        resp = await self.client.post("/", json={"action": "get_worker_adapter_info"})

        self.assertEqual(resp.status, 200)
        data = await resp.json()

        self.assertEqual(data["max_worker_groups"], 2)  # 5 max_workers // 2 workers_per_group
        self.assertEqual(data["workers_per_group"], 2)
        self.assertEqual(data["adapter_type"], "orb-dummy")
        self.assertEqual(data["template_id"], "dummy-template")
        self.assertIn("base_capabilities", data)

    @unittest_run_loop
    async def test_start_worker_group_success(self):
        """Test start_worker_group creates workers and returns IDs."""
        resp = await self.client.post("/", json={"action": "start_worker_group"})

        self.assertEqual(resp.status, 200)
        data = await resp.json()

        self.assertEqual(data["status"], "Worker group started")
        self.assertIn("worker_group_id", data)
        self.assertIn("worker_ids", data)
        self.assertEqual(len(data["worker_ids"]), 2)  # workers_per_group=2
        self.assertIn("request_id", data)

    @unittest_run_loop
    async def test_shutdown_worker_group_success(self):
        """Test shutdown_worker_group terminates workers correctly."""
        # Create a group first
        resp = await self.client.post("/", json={"action": "start_worker_group"})
        data = await resp.json()
        worker_group_id = data["worker_group_id"]

        # Shutdown the group
        resp = await self.client.post("/", json={"action": "shutdown_worker_group", "worker_group_id": worker_group_id})

        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["status"], "Worker group shutdown")

    @unittest_run_loop
    async def test_shutdown_worker_group_missing_id_400(self):
        """Test shutdown without worker_group_id returns 400 Bad Request."""
        resp = await self.client.post("/", json={"action": "shutdown_worker_group"})

        self.assertEqual(resp.status, 400)
        data = await resp.json()
        self.assertIn("error", data)
        self.assertIn("worker_group_id", data["error"].lower())

    @unittest_run_loop
    async def test_shutdown_nonexistent_group_404(self):
        """Test shutdown of non-existent group returns 404 Not Found."""
        resp = await self.client.post("/", json={"action": "shutdown_worker_group", "worker_group_id": "nonexistent"})

        self.assertEqual(resp.status, 404)
        data = await resp.json()
        self.assertIn("error", data)

    @unittest_run_loop
    async def test_get_worker_group_status_success(self):
        """Test get_worker_group_status returns ORB-format status."""
        # Create a group
        resp = await self.client.post("/", json={"action": "start_worker_group"})
        data = await resp.json()
        worker_group_id = data["worker_group_id"]

        # Get status
        resp = await self.client.post(
            "/", json={"action": "get_worker_group_status", "worker_group_id": worker_group_id}
        )

        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertIn("requests", data)
        self.assertEqual(len(data["requests"]), 1)

    @unittest_run_loop
    async def test_get_worker_group_status_not_found_404(self):
        """Test get_worker_group_status for non-existent group returns 404."""
        resp = await self.client.post("/", json={"action": "get_worker_group_status", "worker_group_id": "nonexistent"})

        self.assertEqual(resp.status, 404)

    @unittest_run_loop
    async def test_unknown_action_400(self):
        """Test unknown action returns 400 Bad Request with error message."""
        resp = await self.client.post("/", json={"action": "invalid_action"})

        self.assertEqual(resp.status, 400)
        data = await resp.json()
        self.assertIn("error", data)
        self.assertIn("invalid_action", data["error"].lower())

    @unittest_run_loop
    async def test_missing_action_400(self):
        """Test request without action returns 400 Bad Request."""
        resp = await self.client.post("/", json={})

        self.assertEqual(resp.status, 400)
        data = await resp.json()
        self.assertIn("error", data)

    @unittest_run_loop
    async def test_capacity_exceeded_429(self):
        """Test that exceeding capacity returns 429 Too Many Requests."""
        # Fill capacity: 2 groups × 2 workers = 4 workers (max 5 allows 2 groups)
        for _ in range(2):
            resp = await self.client.post("/", json={"action": "start_worker_group"})
            self.assertEqual(resp.status, 200)

        # Third group should fail with 429
        resp = await self.client.post("/", json={"action": "start_worker_group"})

        self.assertEqual(resp.status, 429)
        data = await resp.json()
        self.assertIn("error", data)


class TestORBWorkerAdapterInterface(unittest.TestCase):
    """Tests for the real ORBWorkerAdapter with mocked ORB API.

    These tests verify that the adapter correctly translates
    Scaler's worker lifecycle into ORB API calls.
    """

    def setUp(self):
        """Set up test fixtures with temporary directories for ORB config."""
        self.temp_dir = tempfile.mkdtemp()
        self.workdir = Path(self.temp_dir) / "orb_workdir"
        self.workdir.mkdir()
        self.templates = Path(self.temp_dir) / "templates.json"
        self.templates.write_text(json.dumps({"templates": [{"templateId": "test-template", "podSpec": "spec.yaml"}]}))

    def tearDown(self):
        """Clean up temporary directories."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _run_async(self, coro):
        """Helper to run async coroutines in synchronous tests."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    def _create_mock_config(self, template_id="test-template"):
        """Create a mock ORBWorkerAdapterConfig object."""
        config = MagicMock()
        config.orb_workdir = str(self.workdir)
        config.orb_templates = str(self.templates)
        config.orb_template_id = template_id
        config.workers_per_group = 2
        config.worker_adapter_config.max_workers = 10
        config.web_config.adapter_web_host = "127.0.0.1"
        config.web_config.adapter_web_port = 8080
        return config

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_initialization_validates_template(self, mock_orb_api):
        """Test that adapter validates template ID exists on startup."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config()
        adapter = ORBWorkerAdapter(config)

        mock_orb_api.get_available_templates.assert_called_once()
        self.assertEqual(adapter._template_id, "test-template")

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_initialization_stores_config_values(self, mock_orb_api):
        """Test that adapter stores configuration values correctly."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config()
        adapter = ORBWorkerAdapter(config)

        self.assertEqual(adapter._workers_per_group, 2)
        self.assertEqual(adapter._max_workers, 10)

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_initialization_invalid_template_raises_error(self, mock_orb_api):
        """Test that invalid template ID raises ValueError on startup."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config(template_id="nonexistent-template")

        with self.assertRaises(ValueError) as ctx:
            ORBWorkerAdapter(config)

        self.assertIn("nonexistent-template", str(ctx.exception))
        self.assertIn("not found", str(ctx.exception))

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_start_worker_group_calls_request_machines(self, mock_orb_api):
        """Test that start_worker_group calls ORB's request_machines API."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}
        mock_orb_api.request_machines.return_value = {"message": "Success", "requestId": "test-request-123"}

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config()
        adapter = ORBWorkerAdapter(config)
        worker_group_id = self._run_async(adapter.start_worker_group())

        self.assertIsNotNone(worker_group_id)
        mock_orb_api.request_machines.assert_called_once()

        # Verify correct parameters passed to ORB
        call_kwargs = mock_orb_api.request_machines.call_args[1]
        self.assertEqual(call_kwargs["count"], 2)
        self.assertEqual(call_kwargs["template_id"], "test-template")
        self.assertEqual(call_kwargs["workdir"], str(self.workdir))

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_shutdown_worker_group_calls_return_machines(self, mock_orb_api):
        """Test that shutdown_worker_group calls ORB's request_return_machines API."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}
        mock_orb_api.request_machines.return_value = {"message": "Success", "requestId": "test-request-123"}
        mock_orb_api.request_return_machines.return_value = {
            "message": "Machines returned.",
            "requestId": "return-test-request-123",
        }

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config()
        adapter = ORBWorkerAdapter(config)
        worker_group_id = self._run_async(adapter.start_worker_group())

        self._run_async(adapter.shutdown_worker_group(worker_group_id))

        mock_orb_api.request_return_machines.assert_called_once()

        # Verify machines list passed to ORB
        call_kwargs = mock_orb_api.request_return_machines.call_args[1]
        self.assertIn("machines", call_kwargs)
        self.assertEqual(len(call_kwargs["machines"]), 2)  # workers_per_group

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_shutdown_nonexistent_group_raises_error(self, mock_orb_api):
        """Test that shutdown of non-existent group raises WorkerGroupNotFoundError."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config()
        adapter = ORBWorkerAdapter(config)

        with self.assertRaises(WorkerGroupNotFoundError):
            self._run_async(adapter.shutdown_worker_group(b"orb-nonexistent"))

    @patch("scaler.worker_adapter.orb_adapter.worker_adapter.orb_api")
    def test_get_worker_group_status_calls_get_request_status(self, mock_orb_api):
        """Test that get_worker_group_status calls ORB's get_request_status API."""
        mock_orb_api.get_available_templates.return_value = {"templates": [{"templateId": "test-template"}]}
        mock_orb_api.request_machines.return_value = {"message": "Success", "requestId": "test-request-123"}
        mock_orb_api.get_request_status.return_value = {
            "requests": [{"requestId": "test-request-123", "status": "complete", "machines": []}]
        }

        from scaler.worker_adapter.orb_adapter import ORBWorkerAdapter

        config = self._create_mock_config()
        adapter = ORBWorkerAdapter(config)
        worker_group_id = self._run_async(adapter.start_worker_group())

        status = self._run_async(adapter.get_worker_group_status(worker_group_id))

        mock_orb_api.get_request_status.assert_called_once()
        self.assertIsNotNone(status)


if __name__ == "__main__":
    unittest.main()
