import unittest

from scaler.scheduler.policies.scaling.estimators.incremental_ratio import IncrementalRatioEstimator
from scaler.scheduler.policies.scaling.estimators.ratio import RatioEstimator
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerManagerID


class TestRatioEstimator(unittest.TestCase):
    """Unit tests for RatioEstimator."""

    def setUp(self):
        self.estimator = RatioEstimator()
        self.manager_id = WorkerManagerID(b"mgr1")
        self.manager = WorkerManagerInfo(
            manager_id=self.manager_id, max_task_concurrency=None, worker_count=0, capabilities={}
        )

    def test_basic_ratio(self):
        """10 tasks with ratio 5 -> desired = ceil(10/5) = 2."""
        self.estimator.update(ClusterState(n_tasks=10, n_workers=0, managers={self.manager_id: self.manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 2)])

    def test_zero_tasks_returns_empty(self):
        """Zero tasks -> empty list (no capsets with tasks)."""
        self.estimator.update(ClusterState(n_tasks=0, n_workers=0, managers={self.manager_id: self.manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [])

    def test_max_concurrency_clamps(self):
        """max_task_concurrency limits the desired count."""
        manager = WorkerManagerInfo(manager_id=self.manager_id, max_task_concurrency=3, worker_count=0, capabilities={})
        self.estimator.update(ClusterState(n_tasks=50, n_workers=0, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 3)])

    def test_multiple_capsets(self):
        """Multiple capsets with tasks produce one entry per capset."""
        gpu_capset = frozenset({"gpu"})
        tpu_capset = frozenset({"tpu"})
        self.estimator.update(
            ClusterState(
                n_tasks=15,
                n_workers=0,
                managers={self.manager_id: self.manager},
                n_tasks_by_capset={gpu_capset: 10, tpu_capset: 5},
                capset_representatives={gpu_capset: {"gpu": 1}, tpu_capset: {"tpu": 1}},
            )
        )

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(len(result), 2)
        result_dict = {frozenset(caps.keys()): (caps, desired) for caps, desired in result}
        self.assertEqual(result_dict[gpu_capset], ({"gpu": 1}, 2))
        self.assertEqual(result_dict[tpu_capset], ({"tpu": 1}, 1))

    def test_skips_empty_capsets(self):
        """Capsets with zero tasks are omitted."""
        gpu_capset = frozenset({"gpu"})
        tpu_capset = frozenset({"tpu"})
        self.estimator.update(
            ClusterState(
                n_tasks=10,
                n_workers=0,
                managers={self.manager_id: self.manager},
                n_tasks_by_capset={gpu_capset: 10, tpu_capset: 0},
                capset_representatives={gpu_capset: {"gpu": 1}, tpu_capset: {"tpu": 1}},
            )
        )

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({"gpu": 1}, 2)])

    def test_uses_capset_representatives(self):
        """Uses capset_representatives for the capability dict."""
        gpu_capset = frozenset({"gpu"})
        self.estimator.update(
            ClusterState(
                n_tasks=10,
                n_workers=0,
                managers={self.manager_id: self.manager},
                n_tasks_by_capset={gpu_capset: 10},
                capset_representatives={gpu_capset: {"gpu": 2, "memory": 4096}},
            )
        )

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({"gpu": 2, "memory": 4096}, 2)])

    def test_per_capset_max_concurrency_clamps(self):
        """max_task_concurrency clamps per-capset desired counts."""
        gpu_capset = frozenset({"gpu"})
        manager = WorkerManagerInfo(manager_id=self.manager_id, max_task_concurrency=3, worker_count=0, capabilities={})
        self.estimator.update(
            ClusterState(
                n_tasks=50,
                n_workers=0,
                managers={self.manager_id: manager},
                n_tasks_by_capset={gpu_capset: 50},
                capset_representatives={gpu_capset: {"gpu": 1}},
            )
        )

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({"gpu": 1}, 3)])

    def test_before_update_returns_empty(self):
        """estimate() before any update() returns an empty list."""
        self.assertEqual(self.estimator.estimate(self.manager_id), [])

    def test_forget_does_not_raise(self):
        """forget() is callable and does not raise."""
        self.estimator.forget(self.manager_id)

    def test_custom_upper_task_ratio(self):
        """Custom upper_task_ratio changes the computation."""
        estimator = RatioEstimator(upper_task_ratio=2)
        estimator.update(ClusterState(n_tasks=10, n_workers=0, managers={self.manager_id: self.manager}))

        result = estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 5)])

    def test_unknown_manager_returns_empty(self):
        """A manager not in managers returns an empty list."""
        self.estimator.update(ClusterState(n_tasks=10, n_workers=0, managers={}))

        result = self.estimator.estimate(WorkerManagerID(b"unknown"))

        self.assertEqual(result, [])


class TestIncrementalRatioEstimator(unittest.TestCase):
    """Unit tests for IncrementalRatioEstimator."""

    def setUp(self):
        self.estimator = IncrementalRatioEstimator()
        self.manager_id = WorkerManagerID(b"mgr1")

    def _make_manager(self, worker_count: int, max_task_concurrency: int | None = None) -> WorkerManagerInfo:
        return WorkerManagerInfo(
            manager_id=self.manager_id,
            max_task_concurrency=max_task_concurrency,
            worker_count=worker_count,
            capabilities={},
        )

    def test_no_workers_no_tasks_returns_current(self):
        """No workers and no tasks: desired equals current managed count."""
        manager = self._make_manager(worker_count=3)
        self.estimator.update(ClusterState(n_tasks=0, n_workers=0, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 3)])

    def test_no_workers_with_tasks_returns_current_plus_one(self):
        """No workers but tasks pending: desired is current + 1 to bootstrap."""
        manager = self._make_manager(worker_count=2)
        self.estimator.update(ClusterState(n_tasks=5, n_workers=0, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 3)])

    def test_high_ratio_returns_current_plus_one(self):
        """Task/worker ratio above upper threshold: desired is current + 1."""
        manager = self._make_manager(worker_count=1)
        self.estimator.update(ClusterState(n_tasks=20, n_workers=1, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 2)])

    def test_low_ratio_with_tasks_returns_floor(self):
        """Task/worker ratio below lower threshold with tasks: desired is max(1, ceil(tasks/upper))."""
        manager = self._make_manager(worker_count=10)
        self.estimator.update(ClusterState(n_tasks=5, n_workers=10, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 1)])

    def test_low_ratio_no_tasks_returns_zero(self):
        """Task/worker ratio below lower threshold with no tasks: desired is 0."""
        manager = self._make_manager(worker_count=5)
        self.estimator.update(ClusterState(n_tasks=0, n_workers=5, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 0)])

    def test_ratio_in_band_returns_current(self):
        """Task/worker ratio within [lower, upper]: desired equals current."""
        manager = self._make_manager(worker_count=5)
        self.estimator.update(ClusterState(n_tasks=15, n_workers=5, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 5)])

    def test_max_concurrency_clamps_desired(self):
        """max_task_concurrency limits the desired count."""
        manager = self._make_manager(worker_count=1, max_task_concurrency=1)
        self.estimator.update(ClusterState(n_tasks=50, n_workers=1, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 1)])

    def test_max_concurrency_none_means_unlimited(self):
        """max_task_concurrency=None does not clamp the desired count."""
        manager = self._make_manager(worker_count=1, max_task_concurrency=None)
        self.estimator.update(ClusterState(n_tasks=100, n_workers=1, managers={self.manager_id: manager}))

        result = self.estimator.estimate(self.manager_id)

        self.assertEqual(result, [({}, 2)])

    def test_raises_on_capability_aware_state(self):
        """Raises NotImplementedError if the cluster state contains per-capset task counts."""
        gpu_capset = frozenset({"gpu"})
        manager = self._make_manager(worker_count=2)
        self.estimator.update(
            ClusterState(
                n_tasks=10, n_workers=2, managers={self.manager_id: manager}, n_tasks_by_capset={gpu_capset: 10}
            )
        )

        with self.assertRaises(NotImplementedError):
            self.estimator.estimate(self.manager_id)

    def test_before_update_returns_zero(self):
        """estimate() before any update() returns [({}, 0)]."""
        self.assertEqual(self.estimator.estimate(self.manager_id), [({}, 0)])

    def test_forget_does_not_raise(self):
        """forget() is callable and does not raise."""
        self.estimator.forget(self.manager_id)

    def test_unknown_manager_returns_zero(self):
        """A manager not in managers returns [({}, 0)]."""
        self.estimator.update(ClusterState(n_tasks=5, n_workers=0, managers={}))

        result = self.estimator.estimate(WorkerManagerID(b"unknown"))

        self.assertEqual(result, [({}, 0)])


if __name__ == "__main__":
    unittest.main()
