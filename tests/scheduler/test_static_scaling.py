import unittest

from scaler.protocol.capnp import WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.controllers.policies.simple_policy.scaling.static import StaticScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.utility import (
    create_scaling_policy,
    parse_scaling_policy_token,
)
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy
from scaler.utility.identifiers import TaskID
from scaler.utility.snapshot import InformationSnapshot

_MANAGER_ID = b"manager_aaa"


def _heartbeat(max_task_concurrency: int) -> WorkerManagerHeartbeat:
    return WorkerManagerHeartbeat(maxTaskConcurrency=max_task_concurrency, capabilities=[], workerManagerID=_MANAGER_ID)


def _desired(policy: StaticScalingPolicy, max_task_concurrency: int) -> int:
    commands = policy.get_scaling_commands(
        InformationSnapshot(tasks={}, workers={}), _heartbeat(max_task_concurrency), [], {}
    )
    requests = list(commands[0].setDesiredTaskConcurrencyRequests)
    return requests[0].taskConcurrency


class TestScalingPolicyTokenParse(unittest.TestCase):
    def test_a_bare_name_has_no_argument(self) -> None:
        self.assertEqual(parse_scaling_policy_token("vanilla"), (ScalingPolicyStrategy.VANILLA, None))
        self.assertEqual(parse_scaling_policy_token("static"), (ScalingPolicyStrategy.STATIC, None))

    def test_static_takes_a_count(self) -> None:
        self.assertEqual(parse_scaling_policy_token("static:8"), (ScalingPolicyStrategy.STATIC, 8))

    def test_static_takes_zero(self) -> None:
        self.assertEqual(parse_scaling_policy_token("static:0"), (ScalingPolicyStrategy.STATIC, 0))

    def test_surrounding_space_is_ignored(self) -> None:
        self.assertEqual(parse_scaling_policy_token(" static : 4 "), (ScalingPolicyStrategy.STATIC, 4))

    def test_another_strategy_refuses_an_argument(self) -> None:
        with self.assertRaises(ValueError):
            parse_scaling_policy_token("vanilla:4")

    def test_a_non_integer_argument_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            parse_scaling_policy_token("static:many")

    def test_an_unknown_strategy_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            parse_scaling_policy_token("no")

    def test_the_factory_builds_each_strategy(self) -> None:
        self.assertIsInstance(create_scaling_policy(ScalingPolicyStrategy.VANILLA), VanillaScalingPolicy)
        self.assertIsInstance(create_scaling_policy(ScalingPolicyStrategy.STATIC, 3), StaticScalingPolicy)


class TestStaticScalingPolicy(unittest.TestCase):
    def test_no_count_follows_what_the_manager_advertises(self) -> None:
        self.assertEqual(_desired(StaticScalingPolicy(), max_task_concurrency=6), 6)

    def test_managers_of_different_sizes_each_get_their_own_number(self) -> None:
        policy = StaticScalingPolicy()
        self.assertEqual(_desired(policy, max_task_concurrency=2), 2)
        self.assertEqual(_desired(policy, max_task_concurrency=9), 9)

    def test_a_count_overrides_what_the_manager_advertises(self) -> None:
        self.assertEqual(_desired(StaticScalingPolicy(1), max_task_concurrency=8), 1)

    def test_a_count_is_still_capped_by_the_manager(self) -> None:
        self.assertEqual(_desired(StaticScalingPolicy(50), max_task_concurrency=4), 4)

    def test_zero_provisions_nothing(self) -> None:
        self.assertEqual(_desired(StaticScalingPolicy(0), max_task_concurrency=8), 0)

    def test_an_unlimited_manager_with_no_count_asks_for_nothing(self) -> None:
        # -1 advertises "no limit", which gives no number to hold steady.
        self.assertEqual(_desired(StaticScalingPolicy(), max_task_concurrency=-1), 0)

    def test_an_unlimited_manager_honours_an_explicit_count(self) -> None:
        self.assertEqual(_desired(StaticScalingPolicy(5), max_task_concurrency=-1), 5)

    def test_the_answer_does_not_move_with_the_task_backlog(self) -> None:
        policy = StaticScalingPolicy(3)
        busy = InformationSnapshot(
            tasks={TaskID(str(i).encode().ljust(16, b"0")): object() for i in range(500)}, workers={}
        )
        commands = policy.get_scaling_commands(busy, _heartbeat(8), [], {})
        self.assertEqual(list(commands[0].setDesiredTaskConcurrencyRequests)[0].taskConcurrency, 3)

    def test_a_negative_count_is_refused(self) -> None:
        with self.assertRaises(ValueError):
            StaticScalingPolicy(-2)

    def test_the_command_targets_the_generic_capability_set(self) -> None:
        commands = StaticScalingPolicy(2).get_scaling_commands(
            InformationSnapshot(tasks={}, workers={}), _heartbeat(4), [], {}
        )
        self.assertEqual(len(commands), 1)
        self.assertIsInstance(commands[0], WorkerManagerCommand)
        request = list(commands[0].setDesiredTaskConcurrencyRequests)[0]
        self.assertEqual(len(list(request.capabilities)), 0)


if __name__ == "__main__":
    unittest.main()
