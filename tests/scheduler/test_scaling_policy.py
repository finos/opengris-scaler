import unittest
from typing import Dict, Optional

from scaler.scheduler.policies.scaling.capability import CapabilityScalingPolicy
from scaler.scheduler.policies.scaling.vanilla import VanillaScalingPolicy
from scaler.scheduler.types import ClusterState, WorkerManagerInfo
from scaler.utility.identifiers import WorkerID, WorkerManagerID

MANAGER_ID = WorkerManagerID(b"manager")
OTHER_MANAGER_ID = WorkerManagerID(b"other-manager")
VANILLA_LOGGER = "scaler.scheduler.policies.scaling.vanilla"
CAPABILITY_LOGGER = "scaler.scheduler.policies.scaling.capability"


def _info(manager_id: WorkerManagerID = MANAGER_ID, worker_count: int = 1) -> WorkerManagerInfo:
    return WorkerManagerInfo(
        manager_id=manager_id, max_task_concurrency=None, worker_count=worker_count, capabilities={}
    )


def _cluster_state(
    task_count: int,
    worker_count: int,
    managers: Dict[WorkerManagerID, WorkerManagerInfo],
    capabilities: Optional[dict] = None,
) -> ClusterState:
    capset = frozenset(capabilities or {})
    return ClusterState(
        n_tasks=task_count,
        n_workers=worker_count,
        n_tasks_by_capset={capset: task_count} if task_count else {},
        capset_representatives={capset: capabilities or {}} if task_count else {},
        managers=managers,
    )


class TestVanillaScalingPolicyLogging(unittest.TestCase):
    """A scaling request is logged once, not on every heartbeat that repeats it.

    A manager takes many heartbeats to reach a new worker count, and never reaches it if it cannot provision
    (no quota, no capacity) -- exactly when the log is worth reading and when repeating it every heartbeat
    would bury everything else.
    """

    def setUp(self) -> None:
        self.policy = VanillaScalingPolicy({})
        self.workers = [WorkerID(b"worker")]

    def __decide(self, task_count: int, worker_count: int) -> int:
        cluster_state = _cluster_state(task_count, worker_count, {MANAGER_ID: _info(worker_count=1)})
        return self.policy._compute_desired_worker_count(MANAGER_ID, cluster_state)

    def test_a_standing_request_is_logged_once(self):
        # far more tasks than workers: the policy asks for one more worker every time it is consulted
        with self.assertLogs(VANILLA_LOGGER) as logs:
            desired = [self.__decide(task_count=100, worker_count=1) for _ in range(5)]

        self.assertEqual(desired, [2] * 5, "the decision itself must not change")
        self.assertEqual(len(logs.output), 1, logs.output)

    def test_a_request_is_logged_again_after_the_count_is_reached(self):
        logger_name = VANILLA_LOGGER

        with self.assertLogs(logger_name) as logs:
            self.__decide(task_count=100, worker_count=1)  # asks for 2
            self.__decide(task_count=5, worker_count=5)  # settled: asks for what it already has
            self.__decide(task_count=100, worker_count=1)  # asks for 2 again, a new request

        self.assertEqual(len(logs.output), 2, logs.output)


class TestCapabilityScalingPolicyLogging(unittest.TestCase):
    """A capability policy that never scales has to be diagnosable from what it asked for."""

    def setUp(self) -> None:
        self.policy = CapabilityScalingPolicy()
        self.workers = [WorkerID(b"worker")]

    def __decide(self, capabilities: dict, task_count: int) -> None:
        cluster_state = _cluster_state(task_count, 0, {MANAGER_ID: _info(worker_count=len(self.workers))}, capabilities)
        self.policy.get_scaling_commands(worker_manager_id=MANAGER_ID, cluster_state=cluster_state)

    def test_a_standing_request_is_logged_once_per_capability_set(self):
        with self.assertLogs(CAPABILITY_LOGGER) as logs:
            for _ in range(5):
                self.__decide({"gpu": 1}, task_count=20)

        self.assertEqual(len(logs.output), 1, logs.output)
        self.assertIn("desired=4", logs.output[0])

    def test_a_changed_request_is_logged(self):
        with self.assertLogs(CAPABILITY_LOGGER) as logs:
            self.__decide({"gpu": 1}, task_count=20)  # asks for 4
            self.__decide({"gpu": 1}, task_count=20)  # same request
            self.__decide({"gpu": 1}, task_count=50)  # asks for 10

        self.assertEqual(len(logs.output), 2, logs.output)

    def test_a_request_for_a_different_capability_set_is_logged(self):
        with self.assertLogs(CAPABILITY_LOGGER) as logs:
            self.__decide({"gpu": 1}, task_count=20)
            self.__decide({"fpga": 1}, task_count=20)

        self.assertEqual(len(logs.output), 2, logs.output)


class TestScalingPolicyForgetsDepartedManagers(unittest.TestCase):
    """What a policy remembers per manager must not outlive the managers themselves.

    A policy hears about a manager only through its heartbeats, so an entry left behind by one that never
    comes back stays for the lifetime of the scheduler. Manager ids are operator-supplied and usually stable,
    but one derived from a pod or instance name is not, and then the map grows without bound.
    """

    @staticmethod
    def _infos(*manager_ids: WorkerManagerID) -> dict:
        return {manager_id: _info(manager_id) for manager_id in manager_ids}

    def test_vanilla_forgets_a_manager_the_scheduler_no_longer_knows(self):
        policy = VanillaScalingPolicy({})

        # asks each manager for one more worker, which is what gets remembered
        policy.get_scaling_commands(
            worker_manager_id=MANAGER_ID, cluster_state=_cluster_state(100, 1, self._infos(MANAGER_ID))
        )
        self.assertEqual(set(policy._logged_desired_by_manager), {MANAGER_ID})

        policy.get_scaling_commands(
            worker_manager_id=OTHER_MANAGER_ID, cluster_state=_cluster_state(100, 1, self._infos(OTHER_MANAGER_ID))
        )
        self.assertEqual(set(policy._logged_desired_by_manager), {OTHER_MANAGER_ID})

    def test_capability_forgets_a_manager_the_scheduler_no_longer_knows(self):
        policy = CapabilityScalingPolicy()

        policy.get_scaling_commands(
            worker_manager_id=MANAGER_ID, cluster_state=_cluster_state(1, 0, self._infos(MANAGER_ID), {"gpu": 1})
        )
        self.assertEqual(set(policy._logged_desired_by_manager), {MANAGER_ID})

        policy.get_scaling_commands(
            worker_manager_id=OTHER_MANAGER_ID,
            cluster_state=_cluster_state(1, 0, self._infos(OTHER_MANAGER_ID), {"gpu": 1}),
        )
        self.assertEqual(set(policy._logged_desired_by_manager), {OTHER_MANAGER_ID})

    def test_a_manager_still_heartbeating_is_kept(self):
        policy = CapabilityScalingPolicy()
        known = self._infos(MANAGER_ID, OTHER_MANAGER_ID)

        policy.get_scaling_commands(worker_manager_id=MANAGER_ID, cluster_state=_cluster_state(1, 0, known, {"gpu": 1}))
        policy.get_scaling_commands(
            worker_manager_id=OTHER_MANAGER_ID, cluster_state=_cluster_state(1, 0, known, {"gpu": 1})
        )

        self.assertEqual(set(policy._logged_desired_by_manager), {MANAGER_ID, OTHER_MANAGER_ID})


if __name__ == "__main__":
    unittest.main()
