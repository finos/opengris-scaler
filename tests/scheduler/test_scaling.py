import os
import signal
import time
import unittest
from multiprocessing import Process
from unittest.mock import AsyncMock, MagicMock

from scaler import Client
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
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
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.scheduler import PolicyConfig
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.protocol.capnp import (
    Resource,
    Task,
    WorkerHeartbeat,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
)
from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy
from scaler.scheduler.controllers.worker_manager_controller import WorkerManagerController
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.snapshot import InformationSnapshot
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager
from tests.utility.utility import logging_test_name


class TestScaling(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_address = AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}")

    def test_scaling_basic(self):
        object_storage = ObjectStorageServerProcess(
            bind_address=self.object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_address,
            advertised_object_storage_address=None,
            monitor_address=None,
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=vanilla"),
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

        manager_process = Process(target=_run_native_worker_manager, args=(self.scheduler_address,))
        manager_process.start()

        with Client(self.scheduler_address) as client:
            client.map(time.sleep, [0.1] * 100)

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        manager_process.terminate()
        manager_process.join()

    def test_capability_scaling_basic(self):
        """Test that capability scaling starts workers with the correct capabilities."""
        object_storage = ObjectStorageServerProcess(
            bind_address=self.object_storage_address,
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        object_storage.start()
        object_storage.wait_until_ready()

        scheduler = SchedulerProcess(
            bind_address=AddressConfig.from_string(self.scheduler_address),
            object_storage_address=self.object_storage_address,
            advertised_object_storage_address=None,
            monitor_address=None,
            io_threads=DEFAULT_IO_THREADS,
            max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
            client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
            object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
            load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
            load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
            protected=False,
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=capability"),
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        manager_process = Process(target=_run_native_worker_manager, args=(self.scheduler_address,))
        manager_process.start()

        with Client(self.scheduler_address) as client:
            # Submit tasks without capabilities (should work like vanilla)
            client.map(time.sleep, [0.1] * 50)

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        manager_process.terminate()
        manager_process.join()


class TestCapabilityScalingPolicy(unittest.TestCase):
    """Unit tests for CapabilityScalingPolicy with stateless interface."""

    def setUp(self):
        setup_logger()
        self.policy = CapabilityScalingPolicy()
        # Empty initial state
        self.managed_worker_ids = []
        self.managed_worker_capabilities = {}

    def test_starts_worker_when_no_capable_workers(self):
        """Test that a worker is started when tasks require capabilities no worker provides."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={})
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(
            information_snapshot,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.startWorkers)
        self.assertEqual(commands[0].capabilities, {"gpu": 1})

    def test_no_scale_when_capable_workers_exist(self):
        """Test that no worker is started when workers with matching capabilities exist."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})
        worker_id = WorkerID(b"worker-1")
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=0)

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={worker_id: worker_heartbeat})
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(
            information_snapshot,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        # Should not return any start commands
        start_commands = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_commands), 0)

    def test_scales_when_task_ratio_exceeds_threshold(self):
        """Test that scaling occurs when task-to-worker ratio exceeds upper threshold."""
        # Create 15 tasks with gpu capability (ratio will be 15/1 = 15 > 10)
        tasks = {}
        for _ in range(15):
            task_id = TaskID.generate_task_id()
            tasks[task_id] = _create_mock_task(task_id, {"gpu": 1})

        worker_id = WorkerID(b"worker-1")
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=5)

        information_snapshot = InformationSnapshot(tasks=tasks, workers={worker_id: worker_heartbeat})
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(
            information_snapshot,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.startWorkers)
        self.assertEqual(commands[0].capabilities, {"gpu": 1})

    def test_different_capability_sets_handled_separately(self):
        """Test that tasks with different capabilities trigger separate scaling suggestions."""
        gpu_task_id = TaskID.generate_task_id()
        gpu_task = _create_mock_task(gpu_task_id, {"gpu": 1})

        tpu_task_id = TaskID.generate_task_id()
        tpu_task = _create_mock_task(tpu_task_id, {"tpu": 1})

        information_snapshot = InformationSnapshot(tasks={gpu_task_id: gpu_task, tpu_task_id: tpu_task}, workers={})
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        # Should return 2 start commands (one for each capability set)
        commands = self.policy.get_scaling_commands(
            information_snapshot,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        start_commands = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_commands), 2)

        capabilities_requested = {frozenset(c.capabilities.keys()) for c in start_commands}
        self.assertIn(frozenset({"gpu"}), capabilities_requested)
        self.assertIn(frozenset({"tpu"}), capabilities_requested)

    def test_worker_with_superset_capabilities_matches_task(self):
        """Test that a worker with superset capabilities can handle tasks requiring a subset."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})  # Task requires only GPU

        worker_id = WorkerID(b"worker-1")
        # Worker has both GPU and CPU capabilities
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1, "cpu": -1}, queued_tasks=0)

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={worker_id: worker_heartbeat})
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(
            information_snapshot,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        # No StartWorkers command should be returned
        start_commands = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_commands), 0)

    def test_tasks_without_capabilities_handled(self):
        """Test that tasks without capability requirements are handled correctly."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {})  # No capabilities required

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={})  # No workers
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(
            information_snapshot,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        # Should start a worker with empty capabilities
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.startWorkers)
        self.assertEqual(commands[0].capabilities, {})

    def test_get_status_returns_scaling_manager_status(self):
        """Test that get_status returns a ScalingManagerStatus object."""
        managed_workers = {b"mgr-1": [WorkerID(b"worker-1"), WorkerID(b"worker-2")]}

        status = self.policy.get_status(managed_workers)

        from scaler.protocol.capnp import ScalingManagerStatus

        self.assertIsInstance(status, ScalingManagerStatus)

    def test_no_duplicate_worker_for_pending_workers(self):
        """Test that no new worker is started if capable workers are already pending."""
        # First snapshot: task with mqa:1, no workers -> should start a worker
        task1_id = TaskID.generate_task_id()
        task1 = _create_mock_task(task1_id, {"mqa": 1})
        information_snapshot1 = InformationSnapshot(tasks={task1_id: task1}, workers={})
        worker_manager_heartbeat = _create_worker_manager_heartbeat(b"test")

        commands1 = self.policy.get_scaling_commands(
            information_snapshot1,
            worker_manager_heartbeat,
            self.managed_worker_ids,
            self.managed_worker_capabilities,
            {},
        )

        self.assertEqual(len(commands1), 1)
        self.assertEqual(commands1[0].command, WorkerManagerCommandType.startWorkers)
        self.assertEqual(commands1[0].capabilities, {"mqa": 1})

        # Simulate state update as if manager responded successfully
        updated_worker_ids = [WorkerID(b"w-mqa")]
        updated_capabilities = {"mqa": -1}

        # Second snapshot: task with mqa:-1, no workers connected yet
        # Should NOT start another worker since capable worker already pending
        task2_id = TaskID.generate_task_id()
        task2 = _create_mock_task(task2_id, {"mqa": -1})
        information_snapshot2 = InformationSnapshot(tasks={task2_id: task2}, workers={})

        commands2 = self.policy.get_scaling_commands(
            information_snapshot2, worker_manager_heartbeat, updated_worker_ids, updated_capabilities, {}
        )

        # No StartWorkers command should be returned
        start_commands = [c for c in commands2 if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_commands), 0, "Should not start new worker when capable worker is pending")

    def test_greedy_shutdown_multiple_same_capability(self):
        """0 tasks, 3 workers with same capability -> all shut down in one command."""
        workers = {}
        managed = []
        for i in range(3):
            wid = WorkerID(f"gpu-w{i}".encode())
            workers[wid] = _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=i)
            managed.append(wid)

        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {})

        shutdown_commands = [c for c in commands if c.command == WorkerManagerCommandType.shutdownWorkers]
        self.assertEqual(len(shutdown_commands), 1)
        self.assertEqual(len(shutdown_commands[0].workerIDs), 3)

    def test_shutdown_allows_duplicate_worker_ids(self):
        """Worker appearing in multiple capability groups may appear multiple times in shutdown."""
        # Worker w0 has {gpu, cpu}, w1 has {gpu}. Both capability groups trigger shutdown.
        # w0 may appear in shutdown for both {gpu} and {gpu, cpu} groups — duplicates are allowed.
        w0 = WorkerID(b"w0-gpu-cpu")
        w1 = WorkerID(b"w1-gpu")
        workers = {
            w0: _create_mock_worker_heartbeat({"gpu": -1, "cpu": -1}, queued_tasks=0),
            w1: _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=1),
        }
        managed = [w0, w1]

        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {})

        shutdown_commands = [c for c in commands if c.command == WorkerManagerCommandType.shutdownWorkers]
        self.assertEqual(len(shutdown_commands), 1)
        shutdown_ids = shutdown_commands[0].workerIDs
        # Both workers should be present; duplicates are acceptable
        self.assertTrue(len(shutdown_ids) >= 2)
        self.assertIn(bytes(w0), shutdown_ids)
        self.assertIn(bytes(w1), shutdown_ids)

    def test_pending_fills_max_suppresses_capability_start(self):
        """When pending workers reach max_concurrency, no StartWorkers should be issued."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})
        snapshot = InformationSnapshot(tasks={task_id: task}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=3)
        manager_snapshot = WorkerManagerSnapshot(
            worker_manager_id=b"mgr", max_task_concurrency=3, worker_count=0, last_seen_s=0.0, pending_worker_count=3
        )

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, [], {}, {b"mgr": manager_snapshot})

        start_cmds = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_cmds), 0)

    def test_managed_plus_pending_below_max_allows_capability_start(self):
        """When managed + pending < max_concurrency, capability StartWorkers should be issued."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})
        managed = [WorkerID(b"w0")]
        snapshot = InformationSnapshot(tasks={task_id: task}, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=5)
        manager_snapshot = WorkerManagerSnapshot(
            worker_manager_id=b"mgr",
            max_task_concurrency=5,
            worker_count=1,
            last_seen_s=0.0,
            pending_worker_count=2,  # 1 + 2 == 3 < 5
        )

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {b"mgr": manager_snapshot})

        start_cmds = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_cmds), 1)
        self.assertEqual(start_cmds[0].capabilities, {"gpu": 1})


class TestVanillaScalingPolicy(unittest.TestCase):
    """Unit tests for VanillaScalingPolicy greedy shutdown."""

    def setUp(self):
        setup_logger()
        self.policy = VanillaScalingPolicy()

    def test_greedy_shutdown_all_idle(self):
        """0 tasks, 4 workers with varying busyness -> all 4 shut down in one command."""
        workers = {
            WorkerID(b"w0"): _create_mock_worker_heartbeat({}, queued_tasks=0),
            WorkerID(b"w1"): _create_mock_worker_heartbeat({}, queued_tasks=1),
            WorkerID(b"w2"): _create_mock_worker_heartbeat({}, queued_tasks=2),
            WorkerID(b"w3"): _create_mock_worker_heartbeat({}, queued_tasks=5),
        }
        snapshot = InformationSnapshot(tasks={}, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"test")
        managed = list(workers.keys())

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {})

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.shutdownWorkers)
        self.assertEqual(len(commands[0].workerIDs), 4)

    def test_greedy_shutdown_partial(self):
        """5 tasks, 10 workers -> shutdown 9, keep 1 (ceil(5/10)=1)."""
        tasks = {}
        for _ in range(5):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {})

        workers = {}
        managed = []
        for i in range(10):
            wid = WorkerID(f"w{i}".encode())
            workers[wid] = _create_mock_worker_heartbeat({}, queued_tasks=i)
            managed.append(wid)

        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {})

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerManagerCommandType.shutdownWorkers)
        self.assertEqual(len(commands[0].workerIDs), 9)
        # The busiest worker (w9) should NOT be in the shutdown list
        shutdown_set = set(commands[0].workerIDs)
        self.assertNotIn(bytes(WorkerID(b"w9")), shutdown_set)

    def test_greedy_shutdown_no_action_when_ratio_ok(self):
        """15 tasks, 5 workers (ratio=3 > lower=1) -> no shutdown."""
        tasks = {}
        for _ in range(15):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {})

        workers = {}
        managed = []
        for i in range(5):
            wid = WorkerID(f"w{i}".encode())
            workers[wid] = _create_mock_worker_heartbeat({}, queued_tasks=i)
            managed.append(wid)

        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {})
        self.assertEqual(len(commands), 0)

    def test_greedy_shutdown_worker_ordering(self):
        """Verify least-busy workers are selected first for shutdown."""
        tasks = {}
        for _ in range(8):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {})

        # 10 workers, 8 tasks -> ratio=0.8 < 1 -> shutdown. min_keep = max(1, ceil(8/10))=1. shutdown 9.
        workers = {}
        managed = []
        for i in range(10):
            wid = WorkerID(f"w{i}".encode())
            workers[wid] = _create_mock_worker_heartbeat({}, queued_tasks=i)
            managed.append(wid)

        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"test")

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {})

        self.assertEqual(len(commands), 1)
        # w0..w8 should be shut down (9 least busy), w9 kept
        shutdown_ids = commands[0].workerIDs
        self.assertEqual(len(shutdown_ids), 9)
        self.assertNotIn(bytes(WorkerID(b"w9")), set(shutdown_ids))
        # First in list should be least busy
        self.assertEqual(shutdown_ids[0], bytes(WorkerID(b"w0")))

    def test_pending_fills_max_concurrency_suppresses_start(self):
        """When pending workers alone reach max_concurrency, no StartWorkers should be issued."""
        tasks = {}
        for _ in range(5):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {})
        snapshot = InformationSnapshot(tasks=tasks, workers={})
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=3)
        manager_snapshot = WorkerManagerSnapshot(
            worker_manager_id=b"mgr", max_task_concurrency=3, worker_count=0, last_seen_s=0.0, pending_worker_count=3
        )

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, [], {}, {b"mgr": manager_snapshot})

        start_cmds = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_cmds), 0)

    def test_managed_plus_pending_equals_max_suppresses_start(self):
        """When managed + pending == max_concurrency, no StartWorkers should be issued."""
        tasks = {}
        for _ in range(20):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {})
        managed = [WorkerID(b"w0"), WorkerID(b"w1")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=10) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=5)
        manager_snapshot = WorkerManagerSnapshot(
            worker_manager_id=b"mgr",
            max_task_concurrency=5,
            worker_count=2,
            last_seen_s=0.0,
            pending_worker_count=3,  # 2 + 3 == 5
        )

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {b"mgr": manager_snapshot})

        start_cmds = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_cmds), 0)

    def test_managed_plus_pending_below_max_allows_start(self):
        """When managed + pending < max_concurrency, StartWorkers should still be issued."""
        tasks = {}
        for _ in range(20):
            tid = TaskID.generate_task_id()
            tasks[tid] = _create_mock_task(tid, {})
        managed = [WorkerID(b"w0")]
        workers = {wid: _create_mock_worker_heartbeat({}, queued_tasks=10) for wid in managed}
        snapshot = InformationSnapshot(tasks=tasks, workers=workers)
        heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=5)
        manager_snapshot = WorkerManagerSnapshot(
            worker_manager_id=b"mgr",
            max_task_concurrency=5,
            worker_count=1,
            last_seen_s=0.0,
            pending_worker_count=2,  # 1 + 2 == 3 < 5
        )

        commands = self.policy.get_scaling_commands(snapshot, heartbeat, managed, {}, {b"mgr": manager_snapshot})

        start_cmds = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
        self.assertEqual(len(start_cmds), 1)

    # TODO: uncomment once finos/opengris-scaler#696 is resolved.
    # max_task_concurrency is UInt32 in Cap'n Proto and cannot represent -1, so
    # WorkerManagerHeartbeat.new_msg(max_task_concurrency=-1, ...) currently raises KjException.
    # def test_unlimited_concurrency_ignores_pending(self):
    #     """When max_concurrency == -1, StartWorkers is issued regardless of pending count."""
    #     tid = TaskID.generate_task_id()
    #     snapshot = InformationSnapshot(tasks={tid: _create_mock_task(tid, {})}, workers={})
    #     heartbeat = _create_worker_manager_heartbeat(b"mgr", max_task_concurrency=-1)
    #     manager_snapshot = WorkerManagerSnapshot(
    #         worker_manager_id=b"mgr", max_task_concurrency=-1,
    #         worker_count=0, last_seen_s=0.0, pending_worker_count=1000,
    #     )
    #     commands = self.policy.get_scaling_commands(snapshot, heartbeat, [], {}, {b"mgr": manager_snapshot})
    #     start_cmds = [c for c in commands if c.command == WorkerManagerCommandType.startWorkers]
    #     self.assertEqual(len(start_cmds), 1)


class TestWorkerManagerControllerPendingTracking(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        setup_logger()
        config_controller = MagicMock()
        policy_controller = MagicMock()
        policy_controller.get_scaling_commands.return_value = []
        policy_controller.get_scaling_status.return_value = MagicMock(managed_workers={})

        self.controller = WorkerManagerController(config_controller, policy_controller)
        self.policy_controller = policy_controller

        binder = AsyncMock()
        task_controller = MagicMock()
        task_controller._task_id_to_task = {}
        self.worker_controller = MagicMock()
        self.worker_controller.get_workers_by_manager_id.return_value = []
        self.worker_controller._worker_alive_since = {}

        self.controller.register(binder, task_controller, self.worker_controller)

    async def test_start_success_increments_pending_count(self):
        """A successful StartWorkers response increments _pending_worker_count by 1."""
        source = b"mgr-src"
        self.controller._pending_commands[source] = MagicMock()
        response = WorkerManagerCommandResponse(
            command=WorkerManagerCommandType.startWorkers, status=WorkerManagerCommandResponse.Status.success
        )

        await self.controller.on_command_response(source, response)

        self.assertEqual(self.controller._pending_worker_count.get(source, 0), 1)

    async def test_multiple_start_successes_accumulate_pending(self):
        """Each successful StartWorkers response adds 1 to _pending_worker_count."""
        source = b"mgr-src"
        response = WorkerManagerCommandResponse(
            command=WorkerManagerCommandType.startWorkers, status=WorkerManagerCommandResponse.Status.success
        )

        for _ in range(3):
            self.controller._pending_commands[source] = MagicMock()
            await self.controller.on_command_response(source, response)

        self.assertEqual(self.controller._pending_worker_count.get(source, 0), 3)

    async def test_start_failure_does_not_increment_pending_count(self):
        """A failed StartWorkers response must NOT increment _pending_worker_count."""
        source = b"mgr-src"
        self.controller._pending_commands[source] = MagicMock()
        response = WorkerManagerCommandResponse(
            command=WorkerManagerCommandType.startWorkers, status=WorkerManagerCommandResponse.Status.tooManyWorkers
        )

        await self.controller.on_command_response(source, response)

        self.assertEqual(self.controller._pending_worker_count.get(source, 0), 0)

    async def test_newly_connected_workers_decrement_pending_count(self):
        """When new workers appear, on_heartbeat decrements pending by the newly-connected count."""
        source = b"mgr-src"
        manager_id = b"mgr-id"
        heartbeat = _create_worker_manager_heartbeat(manager_id)
        self.controller._manager_alive_since[source] = (0.0, heartbeat)
        self.controller._manager_id_to_source[manager_id] = source
        self.controller._pending_worker_count[source] = 3
        self.controller._last_worker_count[source] = 0
        self.worker_controller.get_workers_by_manager_id.return_value = [WorkerID(b"w0"), WorkerID(b"w1")]

        await self.controller.on_heartbeat(source, heartbeat)

        self.assertEqual(self.controller._pending_worker_count[source], 1)  # 3 - 2
        self.assertEqual(self.controller._last_worker_count[source], 2)

    async def test_pending_count_clamped_to_zero_on_excess_connections(self):
        """_pending_worker_count must never go negative even if more workers connect than expected."""
        source = b"mgr-src"
        manager_id = b"mgr-id"
        heartbeat = _create_worker_manager_heartbeat(manager_id)
        self.controller._manager_alive_since[source] = (0.0, heartbeat)
        self.controller._manager_id_to_source[manager_id] = source
        self.controller._pending_worker_count[source] = 1
        self.controller._last_worker_count[source] = 0
        self.worker_controller.get_workers_by_manager_id.return_value = [
            WorkerID(b"w0"),
            WorkerID(b"w1"),
            WorkerID(b"w2"),  # 3 connected, only 1 was pending
        ]

        await self.controller.on_heartbeat(source, heartbeat)

        self.assertGreaterEqual(self.controller._pending_worker_count[source], 0)

    async def test_disconnect_clears_pending_state(self):
        """_disconnect_manager removes both _pending_worker_count and _last_worker_count entries."""
        source = b"mgr-src"
        manager_id = b"mgr-id"
        heartbeat = _create_worker_manager_heartbeat(manager_id)
        self.controller._manager_alive_since[source] = (0.0, heartbeat)
        self.controller._manager_id_to_source[manager_id] = source
        self.controller._pending_worker_count[source] = 5
        self.controller._last_worker_count[source] = 3

        await self.controller._disconnect_manager(source)

        self.assertNotIn(source, self.controller._pending_worker_count)
        self.assertNotIn(source, self.controller._last_worker_count)

    def test_get_status_includes_pending_workers_count(self):
        """get_status should expose pending_workers in each worker manager detail dict."""
        source = b"mgr-src"
        manager_id = b"mgr-id"
        heartbeat = _create_worker_manager_heartbeat(manager_id)
        self.controller._manager_alive_since[source] = (0.0, heartbeat)
        self.controller._pending_worker_count[source] = 2
        self.worker_controller.get_workers_by_manager_id.return_value = []

        status = self.controller.get_status()

        detail = next(d for d in status.workerManagerDetails if d["worker_manager_id"] == manager_id)
        self.assertEqual(detail["pending_workers"], 2)


def _create_mock_task(task_id: TaskID, capabilities: dict) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=task_id,
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities,
    )


def _create_mock_worker_heartbeat(capabilities: dict, queued_tasks: int = 0) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=queued_tasks,
        latencyUS=100,
        taskLock=False,
        processors=[],
        capabilities=capabilities,
        workerManagerID=b"test",
    )


def _create_worker_manager_heartbeat(
    worker_manager_id: bytes, max_task_concurrency: int = 10
) -> WorkerManagerHeartbeat:
    return WorkerManagerHeartbeat(
        maxTaskConcurrency=max_task_concurrency, capabilities={}, workerManagerID=worker_manager_id
    )


def _run_native_worker_manager(
    scheduler_address: str, max_task_concurrency: int = 4, worker_manager_id: str = "test_manager"
) -> None:
    manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_manager_id=worker_manager_id,
                object_storage_address=None,
                max_task_concurrency=max_task_concurrency,
            ),
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=10,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
                io_threads=DEFAULT_IO_THREADS,
                event_loop="builtin",
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )

    manager.run()
