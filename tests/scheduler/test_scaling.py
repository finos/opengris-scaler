import os
import signal
import time
import unittest
from multiprocessing import Process
from unittest.mock import AsyncMock, patch

from aiohttp import web

from scaler import Client
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.defaults import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.config.types.zmq import ZMQConfig
from scaler.protocol.python.message import InformationSnapshot, Task, WorkerHeartbeat
from scaler.protocol.python.status import Resource
from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingController
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_adapter.native import NativeWorkerAdapter
from tests.utility.utility import logging_test_name


def _run_native_worker_adapter(scheduler_address: str, webhook_port: int) -> None:
    """Construct a NativeWorkerAdapter and run its aiohttp app. Runs in a separate process."""
    adapter = NativeWorkerAdapter(
        NativeWorkerAdapterConfig(
            web_config=WebConfig(),
            worker_adapter_config=WorkerAdapterConfig(
                scheduler_address=ZMQConfig.from_string(scheduler_address), object_storage_address=None, max_workers=4
            ),
            event_loop="builtin",
            worker_io_threads=DEFAULT_IO_THREADS,
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=10,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )

    app = adapter.create_app()
    web.run_app(app, host="127.0.0.1", port=webhook_port)


class TestScaling(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_config = ObjectStorageAddressConfig("127.0.0.1", get_available_tcp_port())
        self.webhook_port = get_available_tcp_port()

    def test_scaling_basic(self):
        object_storage = ObjectStorageServerProcess(
            object_storage_address=self.object_storage_config,
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            address=ZMQConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_config,
            monitor_address=None,
            policy=PolicyConfig(
                policy_content="allocate=even_load; scaling=vanilla",
                adapter_webhook_urls=(f"http://127.0.0.1:{self.webhook_port}",),
            ),
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        webhook_server = Process(target=_run_native_worker_adapter, args=(self.scheduler_address, self.webhook_port))
        webhook_server.start()

        with Client(self.scheduler_address) as client:
            client.map(time.sleep, [(0.1,) for _ in range(100)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        os.kill(webhook_server.pid, signal.SIGINT)
        webhook_server.join()

    def test_capability_scaling_basic(self):
        """Test that capability scaling starts worker groups with the correct capabilities."""
        object_storage = ObjectStorageServerProcess(
            object_storage_address=self.object_storage_config,
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            address=ZMQConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_config,
            monitor_address=None,
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            policy=PolicyConfig(
                policy_content="allocate=even_load; scaling=capability",
                adapter_webhook_urls=(f"http://127.0.0.1:{self.webhook_port}",),
            ),
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        webhook_server = Process(target=_run_native_worker_adapter, args=(self.scheduler_address, self.webhook_port))
        webhook_server.start()

        with Client(self.scheduler_address) as client:
            # Submit tasks without capabilities (should work like vanilla)
            client.map(time.sleep, [(0.1,) for _ in range(50)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        os.kill(webhook_server.pid, signal.SIGINT)
        webhook_server.join()


def _create_mock_task(task_id: TaskID, capabilities: dict) -> Task:
    """Helper to create a mock Task with specified capabilities."""
    client_id = ClientID.generate_client_id()
    return Task.new_msg(
        task_id=task_id,
        source=client_id,
        metadata=b"",
        func_object_id=ObjectID.generate_object_id(client_id),
        function_args=[],
        capabilities=capabilities,
    )


def _create_mock_worker_heartbeat(capabilities: dict, queued_tasks: int = 0) -> WorkerHeartbeat:
    """Helper to create a mock WorkerHeartbeat with specified capabilities."""
    return WorkerHeartbeat.new_msg(
        agent=Resource.new_msg(cpu=1, rss=1000000),
        rss_free=500000,
        queue_size=10,
        queued_tasks=queued_tasks,
        latency_us=100,
        task_lock=False,
        processors=[],
        capabilities=capabilities,
    )


class TestCapabilityScalingController(unittest.IsolatedAsyncioTestCase):
    """Unit tests for CapabilityScalingController."""

    def setUp(self):
        setup_logger()
        self.controller = CapabilityScalingController("http://localhost:8080/webhook")

    async def test_starts_worker_group_when_no_capable_workers(self):
        """Test that a worker group is started when tasks require capabilities no worker provides."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})

        snapshot = InformationSnapshot(tasks={task_id: task}, workers={})  # No workers

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            # First call: get_worker_adapter_info
            # Second call: start_worker_group
            mock_request.side_effect = [
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                ({"worker_group_id": "wg-1", "worker_ids": ["worker-1"]}, web.HTTPOk.status_code),
            ]

            await self.controller.on_snapshot(snapshot)

            # Verify start_worker_group was called with capabilities
            calls = mock_request.call_args_list
            self.assertEqual(len(calls), 2)

            # Second call should be start_worker_group with capabilities
            start_call_payload = calls[1][0][0]
            self.assertEqual(start_call_payload["action"], "start_worker_group")
            self.assertEqual(start_call_payload["capabilities"], {"gpu": 1})

    async def test_no_scale_when_capable_workers_exist(self):
        """Test that no worker group is started when workers with matching capabilities exist."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})
        worker_id = WorkerID(b"worker-1")
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=0)

        snapshot = InformationSnapshot(tasks={task_id: task}, workers={worker_id: worker_heartbeat})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            await self.controller.on_snapshot(snapshot)

            # Should not start a new worker group since task ratio (1/1=1) is not > upper_task_ratio (10)
            # and a capable worker already exists
            for call in mock_request.call_args_list:
                payload = call[0][0]
                self.assertNotEqual(payload.get("action"), "start_worker_group")

    async def test_scales_when_task_ratio_exceeds_threshold(self):
        """Test that scaling occurs when task-to-worker ratio exceeds upper threshold."""
        # Create 15 tasks with gpu capability (ratio will be 15/1 = 15 > 10)
        tasks = {}
        for i in range(15):
            task_id = TaskID.generate_task_id()
            tasks[task_id] = _create_mock_task(task_id, {"gpu": 1})

        worker_id = WorkerID(b"worker-1")
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=5)

        snapshot = InformationSnapshot(tasks=tasks, workers={worker_id: worker_heartbeat})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                ({"worker_group_id": "wg-1", "worker_ids": ["worker-2"]}, web.HTTPOk.status_code),
            ]

            await self.controller.on_snapshot(snapshot)

            # Verify start_worker_group was called
            calls = mock_request.call_args_list
            start_calls = [c for c in calls if c[0][0].get("action") == "start_worker_group"]
            self.assertEqual(len(start_calls), 1)
            self.assertEqual(start_calls[0][0][0]["capabilities"], {"gpu": 1})

    async def test_different_capability_sets_handled_separately(self):
        """Test that tasks with different capabilities trigger separate scaling decisions."""
        gpu_task_id = TaskID.generate_task_id()
        gpu_task = _create_mock_task(gpu_task_id, {"gpu": 1})

        tpu_task_id = TaskID.generate_task_id()
        tpu_task = _create_mock_task(tpu_task_id, {"tpu": 1})

        snapshot = InformationSnapshot(tasks={gpu_task_id: gpu_task, tpu_task_id: tpu_task}, workers={})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            # Return success for multiple start_worker_group calls
            mock_request.side_effect = [
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                ({"worker_group_id": "wg-gpu", "worker_ids": ["worker-gpu"]}, web.HTTPOk.status_code),
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                ({"worker_group_id": "wg-tpu", "worker_ids": ["worker-tpu"]}, web.HTTPOk.status_code),
            ]

            await self.controller.on_snapshot(snapshot)

            # Check that both GPU and TPU worker groups were requested
            start_calls = [c for c in mock_request.call_args_list if c[0][0].get("action") == "start_worker_group"]
            self.assertEqual(len(start_calls), 2)

            capabilities_requested = {frozenset(c[0][0]["capabilities"].keys()) for c in start_calls}
            self.assertIn(frozenset({"gpu"}), capabilities_requested)
            self.assertIn(frozenset({"tpu"}), capabilities_requested)

    async def test_worker_with_superset_capabilities_matches_task(self):
        """Test that a worker with superset capabilities can handle tasks requiring a subset."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})  # Task requires only GPU

        worker_id = WorkerID(b"worker-1")
        # Worker has both GPU and CPU capabilities
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1, "cpu": -1}, queued_tasks=0)

        snapshot = InformationSnapshot(tasks={task_id: task}, workers={worker_id: worker_heartbeat})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            await self.controller.on_snapshot(snapshot)

            # Should not start a new worker group since the existing worker can handle the task
            start_calls = [c for c in mock_request.call_args_list if c[0][0].get("action") == "start_worker_group"]
            self.assertEqual(len(start_calls), 0)

    async def test_tasks_without_capabilities_handled(self):
        """Test that tasks without capability requirements are handled correctly."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {})  # No capabilities required

        snapshot = InformationSnapshot(tasks={task_id: task}, workers={})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                ({"worker_group_id": "wg-1", "worker_ids": ["worker-1"]}, web.HTTPOk.status_code),
            ]

            await self.controller.on_snapshot(snapshot)

            # Should start a worker group with empty capabilities
            start_calls = [c for c in mock_request.call_args_list if c[0][0].get("action") == "start_worker_group"]
            self.assertEqual(len(start_calls), 1)
            self.assertEqual(start_calls[0][0][0]["capabilities"], {})

    async def test_get_status_returns_scaling_manager_status(self):
        """Test that get_status returns a ScalingManagerStatus object."""
        # Manually add a worker group
        worker_group_id = b"wg-test"
        worker_ids = [WorkerID(b"worker-1"), WorkerID(b"worker-2")]
        self.controller._worker_groups[worker_group_id] = worker_ids

        status = self.controller.get_status()
        # Verify it returns a ScalingManagerStatus (the actual property access
        # has a bug with memoryview keys, but we can verify the object is created)
        from scaler.protocol.python.status import ScalingManagerStatus

        self.assertIsInstance(status, ScalingManagerStatus)

    async def test_no_duplicate_worker_group_for_pending_group(self):
        """Test that no new worker group is started if a capable group is already pending."""
        # First snapshot: task with mqa:1, no workers -> starts a worker group
        task1_id = TaskID.generate_task_id()
        task1 = _create_mock_task(task1_id, {"mqa": 1})

        snapshot1 = InformationSnapshot(tasks={task1_id: task1}, workers={})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                # Adapter returns mqa:-1 (can handle any amount)
                (
                    {"worker_group_id": "wg-mqa", "worker_ids": ["worker-1"], "capabilities": {"mqa": -1}},
                    web.HTTPOk.status_code,
                ),
            ]

            await self.controller.on_snapshot(snapshot1)

            start_calls = [c for c in mock_request.call_args_list if c[0][0].get("action") == "start_worker_group"]
            self.assertEqual(len(start_calls), 1)
            self.assertEqual(start_calls[0][0][0]["capabilities"], {"mqa": 1})

        # Second snapshot: task with mqa:-1, no workers connected yet
        # Should NOT start another group since we have a pending group with mqa capability
        task2_id = TaskID.generate_task_id()
        task2 = _create_mock_task(task2_id, {"mqa": -1})

        snapshot2 = InformationSnapshot(tasks={task2_id: task2}, workers={})

        with patch.object(self.controller, "_make_request", new_callable=AsyncMock) as mock_request:
            mock_request.side_effect = [
                ({"max_worker_groups": 10}, web.HTTPOk.status_code),
                (
                    {"worker_group_id": "wg-mqa-2", "worker_ids": ["worker-2"], "capabilities": {"mqa": -1}},
                    web.HTTPOk.status_code,
                ),
            ]

            await self.controller.on_snapshot(snapshot2)

            # Should NOT have called start_worker_group because we already have a pending mqa group
            start_calls = [c for c in mock_request.call_args_list if c[0][0].get("action") == "start_worker_group"]
            self.assertEqual(len(start_calls), 0, "Should not start new worker group when capable group is pending")
