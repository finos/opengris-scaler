import unittest
from typing import Optional
from unittest.mock import MagicMock

from scaler.protocol.capnp import WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.static import StaticScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.simple_policy import SimplePolicy

MAX_TASK_CONCURRENCY = 8


def _desired_task_concurrency(policy: StaticScalingPolicy) -> int:
    heartbeat = WorkerManagerHeartbeat(maxTaskConcurrency=MAX_TASK_CONCURRENCY, workerManagerID=b"manager")
    commands = policy.get_scaling_commands(MagicMock(), heartbeat, [], {})
    (command,) = commands
    (request,) = command.setDesiredTaskConcurrencyRequests
    return request.taskConcurrency


class TestStaticScalingPolicy(unittest.TestCase):
    def test_default_requests_the_advertised_maximum(self) -> None:
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(None)), MAX_TASK_CONCURRENCY)

    def test_argument_sets_the_count(self) -> None:
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(3)), 3)
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(0)), 0)

    def test_argument_is_capped_at_the_advertised_maximum(self) -> None:
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(100)), MAX_TASK_CONCURRENCY)


class TestSimplePolicyScalingArgument(unittest.TestCase):
    def __scaling_policy(self, scaling: str) -> Optional[StaticScalingPolicy]:
        policy = SimplePolicy(f"allocate=even_load; scaling={scaling}")
        scaling_policy = policy._scaling_policy
        return scaling_policy if isinstance(scaling_policy, StaticScalingPolicy) else None

    def test_static_parses_with_and_without_argument(self) -> None:
        static = self.__scaling_policy("static")
        assert static is not None
        self.assertEqual(_desired_task_concurrency(static), MAX_TASK_CONCURRENCY)

        static = self.__scaling_policy("static:2")
        assert static is not None
        self.assertEqual(_desired_task_concurrency(static), 2)

    def test_invalid_scaling_strings_are_refused(self) -> None:
        for scaling in ["no", "vanilla:3", "static:many", "static:-1"]:
            with self.subTest(scaling=scaling):
                with self.assertRaises(ValueError):
                    SimplePolicy(f"allocate=even_load; scaling={scaling}")
