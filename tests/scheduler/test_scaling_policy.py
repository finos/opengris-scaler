import unittest
from unittest.mock import MagicMock

from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy
from scaler.utility.identifiers import WorkerID

MANAGER_ID = b"manager"


def _heartbeat() -> MagicMock:
    heartbeat = MagicMock()
    heartbeat.workerManagerID = MANAGER_ID
    heartbeat.maxTaskConcurrency = -1
    return heartbeat


def _snapshot(task_count: int, worker_count: int) -> MagicMock:
    snapshot = MagicMock()
    snapshot.tasks = list(range(task_count))
    snapshot.workers = list(range(worker_count))
    return snapshot


class TestVanillaScalingPolicyLogging(unittest.TestCase):
    """A scaling request is logged once, not on every heartbeat that repeats it.

    A manager takes many heartbeats to reach a new worker count, and never reaches it if it cannot provision
    (no quota, no capacity) -- exactly when the log is worth reading and when repeating it every heartbeat
    would bury everything else.
    """

    def setUp(self) -> None:
        self.policy = VanillaScalingPolicy()
        self.workers = [WorkerID(b"worker")]

    def __decide(self, task_count: int, worker_count: int) -> int:
        return self.policy._compute_desired_worker_count(
            _snapshot(task_count, worker_count), _heartbeat(), self.workers
        )

    def test_a_standing_request_is_logged_once(self):
        # far more tasks than workers: the policy asks for one more worker every time it is consulted
        with self.assertLogs("scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla") as logs:
            desired = [self.__decide(task_count=100, worker_count=1) for _ in range(5)]

        self.assertEqual(desired, [2] * 5, "the decision itself must not change")
        self.assertEqual(len(logs.output), 1, logs.output)

    def test_a_request_is_logged_again_after_the_count_is_reached(self):
        logger_name = "scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla"

        with self.assertLogs(logger_name) as logs:
            self.__decide(task_count=100, worker_count=1)  # asks for 2
            self.__decide(task_count=5, worker_count=5)  # settled: asks for what it already has
            self.__decide(task_count=100, worker_count=1)  # asks for 2 again, a new request

        self.assertEqual(len(logs.output), 2, logs.output)


if __name__ == "__main__":
    unittest.main()
