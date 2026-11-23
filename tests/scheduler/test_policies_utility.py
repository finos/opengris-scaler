import unittest
from typing import Dict, Optional

from scaler.protocol.capnp import Resource, Task, WorkerHeartbeat
from scaler.scheduler.policies.scaling.utility import build_cluster_state
from scaler.scheduler.types import WorkerManagerInfo
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID, WorkerManagerID


class TestBuildClusterState(unittest.TestCase):
    """Unit tests for build_cluster_state."""

    def test_empty_state(self):
        """Empty inputs yield zero counts, empty dicts, and the managers passed through."""
        manager_id = WorkerManagerID(b"mgr")
        managers = {manager_id: _create_worker_manager_info(manager_id)}

        state = build_cluster_state({}, {}, managers)

        self.assertEqual(state.n_tasks, 0)
        self.assertEqual(state.n_workers, 0)
        self.assertEqual(state.n_draining_workers, 0)
        self.assertEqual(state.n_tasks_by_capset, {})
        self.assertEqual(state.capset_representatives, {})
        self.assertIs(state.managers, managers)

    def test_tasks_without_capabilities_land_in_empty_capset(self):
        """Tasks with no capabilities are grouped under the empty frozenset."""
        tasks = {TaskID.generate_task_id(): _create_mock_task({}) for _ in range(3)}

        state = build_cluster_state(tasks, {}, {})

        self.assertEqual(state.n_tasks, 3)
        self.assertEqual(state.n_tasks_by_capset, {frozenset(): 3})
        self.assertEqual(state.capset_representatives, {frozenset(): {}})

    def test_multiple_capsets_get_counts_and_first_task_representative(self):
        """Each capset gets its task count and the first task's capabilities dict as representative."""
        gpu_first = _create_mock_task({"gpu": 1})
        gpu_second = _create_mock_task({"gpu": 2})
        tpu = _create_mock_task({"tpu": 1})
        tasks = {
            TaskID.generate_task_id(): gpu_first,
            TaskID.generate_task_id(): gpu_second,
            TaskID.generate_task_id(): tpu,
        }

        state = build_cluster_state(tasks, {}, {})

        self.assertEqual(state.n_tasks, 3)
        self.assertEqual(state.n_tasks_by_capset, {frozenset({"gpu"}): 2, frozenset({"tpu"}): 1})
        self.assertEqual(state.capset_representatives[frozenset({"gpu"})], {"gpu": 1})
        self.assertEqual(state.capset_representatives[frozenset({"tpu"})], {"tpu": 1})

    def test_n_workers_counted_from_workers(self):
        """n_workers is the number of workers passed in."""
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat() for i in range(4)}

        state = build_cluster_state({}, workers, {})

        self.assertEqual(state.n_workers, 4)
        self.assertEqual(state.n_draining_workers, 0)

    def test_n_draining_workers_counted_from_workers(self):
        """Draining workers stay in n_workers and are counted in n_draining_workers."""
        workers = {WorkerID(f"w{i}".encode()): _create_mock_worker_heartbeat(draining=i == 0) for i in range(4)}

        state = build_cluster_state({}, workers, {})

        self.assertEqual(state.n_workers, 4)
        self.assertEqual(state.n_draining_workers, 1)

    def test_managers_passed_through_unchanged(self):
        """The managers dict is passed through unchanged."""
        manager_id = WorkerManagerID(b"mgr")
        managers = {manager_id: _create_worker_manager_info(manager_id, worker_count=7)}

        state = build_cluster_state({}, {}, managers)

        self.assertIs(state.managers, managers)
        self.assertEqual(state.managers[manager_id].worker_count, 7)


def _create_mock_task(capabilities: dict) -> Task:
    client_id = ClientID.generate_client_id()
    return Task(
        taskId=TaskID.generate_task_id(),
        source=client_id,
        metadata=b"",
        funcObjectId=ObjectID.generate_object_id(client_id),
        functionArgs=[],
        capabilities=capabilities,
    )


def _create_worker_manager_info(
    worker_manager_id: WorkerManagerID,
    max_task_concurrency: Optional[int] = 10,
    capabilities: Optional[Dict[str, int]] = None,
    worker_count: int = 0,
) -> WorkerManagerInfo:
    return WorkerManagerInfo(
        manager_id=worker_manager_id,
        max_task_concurrency=max_task_concurrency,
        worker_count=worker_count,
        capabilities=capabilities or {},
    )


def _create_mock_worker_heartbeat(draining: bool = False) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        agent=Resource(cpu=1, rss=1000000),
        rssFree=500000,
        queueSize=10,
        queuedTasks=0,
        latencyMicroseconds=100,
        taskLock=False,
        processors=[],
        capabilities={},
        workerManagerID=WorkerManagerID(b"test"),
        draining=draining,
    )
