import unittest
from typing import Optional
from unittest.mock import MagicMock

from scaler.protocol.capnp import WorkerManagerHeartbeat
from scaler.scheduler.policies.scaling.static import StaticScalingPolicy
from scaler.scheduler.policies.utility import create_policies

MAX_TASK_CONCURRENCY = 8


def _desired_task_concurrency(policy: StaticScalingPolicy, worker_manager_id: bytes = b"manager") -> int:
    heartbeat = WorkerManagerHeartbeat(maxTaskConcurrency=MAX_TASK_CONCURRENCY, workerManagerID=worker_manager_id)
    commands = policy.get_scaling_commands(MagicMock(), heartbeat, [], {})
    (command,) = commands
    (request,) = command.setDesiredTaskConcurrencyRequests
    return request.taskConcurrency


class TestStaticScalingPolicy(unittest.TestCase):
    def test_default_requests_the_advertised_maximum(self) -> None:
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(None, {})), MAX_TASK_CONCURRENCY)

    def test_argument_sets_the_count(self) -> None:
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(3, {})), 3)
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(0, {})), 0)

    def test_argument_is_capped_at_the_advertised_maximum(self) -> None:
        self.assertEqual(_desired_task_concurrency(StaticScalingPolicy(100, {})), MAX_TASK_CONCURRENCY)

    def test_a_named_manager_gets_its_own_count(self) -> None:
        policy = StaticScalingPolicy(None, {b"native": 3, b"ecs": 0})
        self.assertEqual(_desired_task_concurrency(policy, b"native"), 3)
        self.assertEqual(_desired_task_concurrency(policy, b"ecs"), 0)

    def test_an_unnamed_manager_gets_the_advertised_maximum(self) -> None:
        policy = StaticScalingPolicy(None, {b"native": 3})
        self.assertEqual(_desired_task_concurrency(policy, b"other"), MAX_TASK_CONCURRENCY)

    def test_a_named_count_is_capped_at_the_advertised_maximum(self) -> None:
        policy = StaticScalingPolicy(None, {b"native": 100})
        self.assertEqual(_desired_task_concurrency(policy, b"native"), MAX_TASK_CONCURRENCY)


class TestScalingPolicyArgument(unittest.TestCase):
    def __scaling_policy(self, scaling: str) -> Optional[StaticScalingPolicy]:
        _, scaling_policy = create_policies("simple", f"allocate=even_load; scaling={scaling}")
        return scaling_policy if isinstance(scaling_policy, StaticScalingPolicy) else None

    def test_static_parses_with_and_without_argument(self) -> None:
        static = self.__scaling_policy("static")
        assert static is not None
        self.assertEqual(_desired_task_concurrency(static), MAX_TASK_CONCURRENCY)

        static = self.__scaling_policy("static:2")
        assert static is not None
        self.assertEqual(_desired_task_concurrency(static), 2)

    def test_static_parses_per_manager_counts(self) -> None:
        static = self.__scaling_policy("static[native:4, ecs:0]")
        assert static is not None
        self.assertEqual(_desired_task_concurrency(static, b"native"), 4)
        self.assertEqual(_desired_task_concurrency(static, b"ecs"), 0)
        self.assertEqual(_desired_task_concurrency(static, b"other"), MAX_TASK_CONCURRENCY)

    def test_invalid_scaling_strings_are_refused(self) -> None:
        for scaling in [
            "no",
            "vanilla:3",
            "static:many",
            "static:-1",
            "static[]",
            "static[native]",
            "static[native:]",
            "static[native:4",
            "static[:4]",
            "static[native:-1]",
            "static[native:4:8]",
            "static[native:4, native:2]",
            "static:2[native:4]",
        ]:
            with self.subTest(scaling=scaling):
                with self.assertRaises(ValueError):
                    create_policies("simple", f"allocate=even_load; scaling={scaling}")
