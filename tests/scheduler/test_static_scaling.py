import time
import unittest
from typing import Optional
from unittest.mock import MagicMock

from scaler.scheduler.types import WorkerManagerInfo
from scaler.scheduler.policies.scaling.static import StaticScalingPolicy
from scaler.scheduler.policies.utility import create_policies
from scaler.utility.identifiers import WorkerManagerID

MAX_TASK_CONCURRENCY = 8


def _desired_task_concurrency(
    policy: StaticScalingPolicy, worker_manager_id: WorkerManagerID = WorkerManagerID(b"manager")
) -> int:
    info = WorkerManagerInfo(
        manager_id=worker_manager_id,
        source=b"test",
        last_seen_at=time.time(),
        capabilities={},
        max_task_concurrency=MAX_TASK_CONCURRENCY,
    )
    commands = policy.get_scaling_commands(
        worker_manager_id=worker_manager_id,
        information_snapshot=MagicMock(),
        managed_worker_ids=[],
        worker_manager_infos={worker_manager_id: info},
        worker_count_by_manager={worker_manager_id: 0},
    )
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
        policy = StaticScalingPolicy(None, {WorkerManagerID(b"native"): 3, WorkerManagerID(b"ecs"): 0})
        self.assertEqual(_desired_task_concurrency(policy, WorkerManagerID(b"native")), 3)
        self.assertEqual(_desired_task_concurrency(policy, WorkerManagerID(b"ecs")), 0)

    def test_an_unnamed_manager_gets_the_advertised_maximum(self) -> None:
        policy = StaticScalingPolicy(None, {WorkerManagerID(b"native"): 3})
        self.assertEqual(_desired_task_concurrency(policy, WorkerManagerID(b"other")), MAX_TASK_CONCURRENCY)

    def test_a_named_count_is_capped_at_the_advertised_maximum(self) -> None:
        policy = StaticScalingPolicy(None, {WorkerManagerID(b"native"): 100})
        self.assertEqual(_desired_task_concurrency(policy, WorkerManagerID(b"native")), MAX_TASK_CONCURRENCY)


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
        self.assertEqual(_desired_task_concurrency(static, WorkerManagerID(b"native")), 4)
        self.assertEqual(_desired_task_concurrency(static, WorkerManagerID(b"ecs")), 0)
        self.assertEqual(_desired_task_concurrency(static, WorkerManagerID(b"other")), MAX_TASK_CONCURRENCY)

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
