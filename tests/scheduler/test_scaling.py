import os
import signal
import time
import unittest
from multiprocessing import Process

from scaler import Client
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.config.common.logging import LoggingConfig
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
from scaler.protocol.python.message import (
    InformationSnapshot,
    Task,
    WorkerAdapterCommandType,
    WorkerAdapterHeartbeat,
    WorkerHeartbeat,
)
from scaler.protocol.python.status import Resource
from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerGroupCapabilities, WorkerGroupState
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_adapter.native import NativeWorkerAdapter
from tests.utility.utility import logging_test_name


def _run_native_worker_adapter(scheduler_address: str, max_workers: int = 4) -> None:
    """Construct a NativeWorkerAdapter and run it. Runs in a separate process."""
    adapter = NativeWorkerAdapter(
        NativeWorkerAdapterConfig(
            worker_adapter_config=WorkerAdapterConfig(
                scheduler_address=ZMQConfig.from_string(scheduler_address),
                object_storage_address=None,
                max_workers=max_workers,
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

    adapter.run()


class TestScaling(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_config = ObjectStorageAddressConfig("127.0.0.1", get_available_tcp_port())

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

        adapter_process = Process(target=_run_native_worker_adapter, args=(self.scheduler_address,))
        adapter_process.start()

        with Client(self.scheduler_address) as client:
            client.map(time.sleep, [(0.1,) for _ in range(100)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        adapter_process.terminate()
        adapter_process.join()

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
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=capability"),
            event_loop="builtin",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        scheduler.start()

        adapter_process = Process(target=_run_native_worker_adapter, args=(self.scheduler_address,))
        adapter_process.start()

        with Client(self.scheduler_address) as client:
            # Submit tasks without capabilities (should work like vanilla)
            client.map(time.sleep, [(0.1,) for _ in range(50)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        adapter_process.terminate()
        adapter_process.join()


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


def _create_adapter_heartbeat(max_worker_groups: int = 10) -> WorkerAdapterHeartbeat:
    """Helper to create a mock WorkerAdapterHeartbeat."""
    return WorkerAdapterHeartbeat.new_msg(max_worker_groups=max_worker_groups, workers_per_group=1, capabilities={})


class TestCapabilityScalingController(unittest.TestCase):
    """Unit tests for CapabilityScalingController with stateless interface."""

    def setUp(self):
        setup_logger()
        self.controller = CapabilityScalingController()
        # Empty initial state
        self.worker_groups: WorkerGroupState = {}
        self.worker_group_capabilities: WorkerGroupCapabilities = {}

    def test_starts_worker_group_when_no_capable_workers(self):
        """Test that a worker group is started when tasks require capabilities no worker provides."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={})
        adapter_heartbeat = _create_adapter_heartbeat()

        commands = self.controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)
        self.assertEqual(commands[0].capabilities, {"gpu": 1})

    def test_no_scale_when_capable_workers_exist(self):
        """Test that no worker group is started when workers with matching capabilities exist."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {"gpu": 1})
        worker_id = WorkerID(b"worker-1")
        worker_heartbeat = _create_mock_worker_heartbeat({"gpu": -1}, queued_tasks=0)

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={worker_id: worker_heartbeat})
        adapter_heartbeat = _create_adapter_heartbeat()

        commands = self.controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        # Should not return any start commands
        start_commands = [c for c in commands if c.command == WorkerAdapterCommandType.StartWorkerGroup]
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
        adapter_heartbeat = _create_adapter_heartbeat()

        commands = self.controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)
        self.assertEqual(commands[0].capabilities, {"gpu": 1})

    def test_different_capability_sets_handled_separately(self):
        """Test that tasks with different capabilities trigger separate scaling decisions."""
        gpu_task_id = TaskID.generate_task_id()
        gpu_task = _create_mock_task(gpu_task_id, {"gpu": 1})

        tpu_task_id = TaskID.generate_task_id()
        tpu_task = _create_mock_task(tpu_task_id, {"tpu": 1})

        information_snapshot = InformationSnapshot(tasks={gpu_task_id: gpu_task, tpu_task_id: tpu_task}, workers={})
        adapter_heartbeat = _create_adapter_heartbeat()

        # Should return 2 start commands (one for each capability set)
        commands = self.controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        start_commands = [c for c in commands if c.command == WorkerAdapterCommandType.StartWorkerGroup]
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
        adapter_heartbeat = _create_adapter_heartbeat()

        commands = self.controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        # No StartWorkerGroup command should be returned
        start_commands = [c for c in commands if c.command == WorkerAdapterCommandType.StartWorkerGroup]
        self.assertEqual(len(start_commands), 0)

    def test_tasks_without_capabilities_handled(self):
        """Test that tasks without capability requirements are handled correctly."""
        task_id = TaskID.generate_task_id()
        task = _create_mock_task(task_id, {})  # No capabilities required

        information_snapshot = InformationSnapshot(tasks={task_id: task}, workers={})  # No workers
        adapter_heartbeat = _create_adapter_heartbeat()

        commands = self.controller.get_scaling_commands(
            information_snapshot, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        # Should start a worker group with empty capabilities
        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0].command, WorkerAdapterCommandType.StartWorkerGroup)
        self.assertEqual(commands[0].capabilities, {})

    def test_get_status_returns_scaling_manager_status(self):
        """Test that get_status returns a ScalingManagerStatus object."""
        worker_groups: WorkerGroupState = {b"wg-test": [WorkerID(b"worker-1"), WorkerID(b"worker-2")]}

        status = self.controller.get_status(worker_groups)

        from scaler.protocol.python.status import ScalingManagerStatus

        self.assertIsInstance(status, ScalingManagerStatus)

    def test_no_duplicate_worker_group_for_pending_group(self):
        """Test that no new worker group is started if a capable group is already pending."""
        # First snapshot: task with mqa:1, no workers -> should start a worker group
        task1_id = TaskID.generate_task_id()
        task1 = _create_mock_task(task1_id, {"mqa": 1})
        information_snapshot1 = InformationSnapshot(tasks={task1_id: task1}, workers={})
        adapter_heartbeat = _create_adapter_heartbeat()

        commands1 = self.controller.get_scaling_commands(
            information_snapshot1, adapter_heartbeat, self.worker_groups, self.worker_group_capabilities
        )

        self.assertEqual(len(commands1), 1)
        self.assertEqual(commands1[0].command, WorkerAdapterCommandType.StartWorkerGroup)
        self.assertEqual(commands1[0].capabilities, {"mqa": 1})

        # Simulate state update as if adapter responded successfully
        updated_groups: WorkerGroupState = {b"wg-mqa": []}
        updated_caps: WorkerGroupCapabilities = {b"wg-mqa": {"mqa": -1}}

        # Second snapshot: task with mqa:-1, no workers connected yet
        # Should NOT start another group since capable group already exists
        task2_id = TaskID.generate_task_id()
        task2 = _create_mock_task(task2_id, {"mqa": -1})
        information_snapshot2 = InformationSnapshot(tasks={task2_id: task2}, workers={})

        commands2 = self.controller.get_scaling_commands(
            information_snapshot2, adapter_heartbeat, updated_groups, updated_caps
        )

        # No StartWorkerGroup command should be returned
        start_commands = [c for c in commands2 if c.command == WorkerAdapterCommandType.StartWorkerGroup]
        self.assertEqual(len(start_commands), 0, "Should not start new worker group when capable group is pending")


class TestFixedElasticScaling(unittest.TestCase):
    """Integration tests for FixedElasticScalingController with multiple worker adapters."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.scheduler_address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self.object_storage_config = ObjectStorageAddressConfig("127.0.0.1", get_available_tcp_port())

    def test_fixed_elastic_with_multiple_adapters(self):
        """Test that fixed_elastic scaling works with primary (1 worker) and secondary (4 workers) adapters."""
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
            policy=PolicyConfig(policy_content="allocate=even_load; scaling=fixed_elastic"),
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

        # Start primary adapter with max_workers=1
        primary_adapter_process = Process(
            target=_run_native_worker_adapter, args=(self.scheduler_address,), kwargs={"max_workers": 1}
        )
        primary_adapter_process.start()

        # Start secondary adapter with max_workers=4
        secondary_adapter_process = Process(
            target=_run_native_worker_adapter, args=(self.scheduler_address,), kwargs={"max_workers": 4}
        )
        secondary_adapter_process.start()

        with Client(self.scheduler_address) as client:
            # Submit tasks to trigger scaling
            client.map(time.sleep, [(0.1,) for _ in range(100)])

        os.kill(scheduler.pid, signal.SIGINT)
        scheduler.join()

        object_storage.kill()
        object_storage.join()

        primary_adapter_process.terminate()
        primary_adapter_process.join()

        secondary_adapter_process.terminate()
        secondary_adapter_process.join()
