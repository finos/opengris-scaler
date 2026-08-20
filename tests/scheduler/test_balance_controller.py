import unittest
from unittest.mock import MagicMock

from scaler.scheduler.controllers.balance_controller import VanillaBalanceController
from scaler.utility.identifiers import TaskID, WorkerID


class TestBalanceControllerTriggersOnce(unittest.TestCase):
    """Stable balance advice must trigger a balance exactly once, not on every cycle.

    Acting on every cycle once the advice is stable re-issues moves that are still in flight: if a saturated
    worker is slow to confirm a balance-cancel, the same task is re-advised forever ("balancing task" spam +
    invalid transitions). Acting once and waiting for the advice to change breaks that loop.
    """

    @staticmethod
    def _controller(trigger_times: int) -> VanillaBalanceController:
        config = MagicMock()
        config.get_config.return_value = trigger_times
        return VanillaBalanceController(config_controller=config, policy_controller=MagicMock())

    def test_stable_advice_triggers_exactly_once(self):
        balancer = self._controller(trigger_times=2)
        should_balance = balancer._VanillaBalanceController__should_balance  # type: ignore[attr-defined]

        advice = {WorkerID(b"worker"): [TaskID(b"task")]}
        results = [should_balance(advice) for _ in range(6)]

        self.assertEqual(results.count(True), 1, f"expected exactly one trigger, got {results}")

    def test_changed_advice_retriggers(self):
        balancer = self._controller(trigger_times=1)
        should_balance = balancer._VanillaBalanceController__should_balance  # type: ignore[attr-defined]

        advice_a = {WorkerID(b"w"): [TaskID(b"a")]}
        advice_b = {WorkerID(b"w"): [TaskID(b"b")]}
        results = [should_balance(advice) for advice in (advice_a, advice_a, advice_b, advice_b)]

        # each distinct advice becomes actionable once it stabilizes
        self.assertEqual(results, [False, True, False, True], results)

    def test_empty_advice_never_triggers(self):
        balancer = self._controller(trigger_times=1)
        should_balance = balancer._VanillaBalanceController__should_balance  # type: ignore[attr-defined]

        results = [should_balance({}) for _ in range(4)]
        self.assertNotIn(True, results, results)


if __name__ == "__main__":
    unittest.main()
