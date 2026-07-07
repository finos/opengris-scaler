import unittest
from unittest.mock import patch

from scaler.scheduler.controllers.policies.library.scale_down_cooldown import (
    DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS,
    ScaleDownCooldown,
)

_TIME_PATH = "scaler.scheduler.controllers.policies.library.scale_down_cooldown.time"


class TestScaleDownCooldownApply(unittest.TestCase):
    """Unit tests for ScaleDownCooldown.apply(), the real-current-count gate."""

    def setUp(self):
        self.cooldown = ScaleDownCooldown()

    def test_scale_up_applies_immediately(self):
        """desired >= current: no cooldown involved, no call to time.time()."""
        with patch(_TIME_PATH) as mock_time:
            self.assertEqual(self.cooldown.apply(5, 3), 5)
            mock_time.time.assert_not_called()

    def test_first_scale_down_held_at_current(self):
        """The first observation wanting scale-down is held at current; cooldown starts."""
        with patch(_TIME_PATH) as mock_time:
            mock_time.time.return_value = 0.0
            self.assertEqual(self.cooldown.apply(0, 3), 3)

    def test_scale_down_still_held_before_cooldown_elapses(self):
        with patch(_TIME_PATH) as mock_time:
            mock_time.time.side_effect = [0.0, DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS - 1]
            self.cooldown.apply(0, 3)  # starts cooldown at t=0
            self.assertEqual(self.cooldown.apply(0, 3), 3)

    def test_scale_down_applied_after_cooldown_elapses(self):
        with patch(_TIME_PATH) as mock_time:
            mock_time.time.side_effect = [0.0, DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS + 1]
            self.cooldown.apply(0, 3)  # starts cooldown at t=0
            self.assertEqual(self.cooldown.apply(0, 3), 0)

    def test_scale_up_resets_cooldown(self):
        with patch(_TIME_PATH) as mock_time:
            past_cooldown = DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS + 1
            mock_time.time.side_effect = [0.0, past_cooldown]
            self.cooldown.apply(0, 3)  # t=0: cooldown starts
            self.cooldown.apply(5, 3)  # scale-up: resets cooldown, no time.time() call
            result = self.cooldown.apply(0, 3)  # t=past_cooldown: fresh cooldown, still held
        self.assertEqual(result, 3)


class TestScaleDownCooldownReconcile(unittest.TestCase):
    """Unit tests for ScaleDownCooldown.reconcile(), the declarative per-capset gate."""

    def setUp(self):
        self.cooldown = ScaleDownCooldown()

    def test_first_sighting_not_gated(self):
        """A capset seen for the first time has nothing to protect, so it passes through."""
        result = self.cooldown.reconcile([({"gpu": 1}, 5)])
        self.assertEqual(result, [({"gpu": 1}, 5)])

    def test_shrink_held_then_applied(self):
        with patch(_TIME_PATH) as mock_time:
            mock_time.time.side_effect = [0.0, DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS + 1]
            self.cooldown.reconcile([({"gpu": 1}, 10)])  # first sighting, held=10
            held = self.cooldown.reconcile([({"gpu": 1}, 2)])  # t=0: shrink held at 10
            applied = self.cooldown.reconcile([({"gpu": 1}, 2)])  # t=past cooldown: shrink applied

        self.assertEqual(held, [({"gpu": 1}, 10)])
        self.assertEqual(applied, [({"gpu": 1}, 2)])

    def test_capset_vanishing_is_held_then_drained(self):
        """A capset with no more tasks is omitted from the input entirely; this must be
        treated the same as an explicit drop to 0 -- held at its last value, then omitted
        from the output once the cooldown elapses (equivalent to an explicit 0)."""
        with patch(_TIME_PATH) as mock_time:
            mock_time.time.side_effect = [0.0, DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS + 1]
            self.cooldown.reconcile([({"gpu": 1}, 4)])  # first sighting, held=4
            held = self.cooldown.reconcile([])  # t=0: capset vanished, still held
            drained = self.cooldown.reconcile([])  # t=past cooldown: fully drained

        self.assertEqual(held, [({"gpu": 1}, 4)])
        self.assertEqual(drained, [])

    def test_scale_up_resets_cooldown(self):
        with patch(_TIME_PATH) as mock_time:
            past_cooldown = DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS + 1
            mock_time.time.side_effect = [0.0, past_cooldown]
            self.cooldown.reconcile([({"gpu": 1}, 10)])  # first sighting, held=10
            self.cooldown.reconcile([({"gpu": 1}, 2)])  # t=0: shrink, cooldown starts
            self.cooldown.reconcile([({"gpu": 1}, 20)])  # scale-up: resets cooldown, no time.time() call
            result = self.cooldown.reconcile([({"gpu": 1}, 2)])  # t=past_cooldown: fresh cooldown, still held

        self.assertEqual(result, [({"gpu": 1}, 20)])

    def test_independent_capsets_do_not_interfere(self):
        """One capset's cooldown must not affect a different capset in the same round."""
        with patch(_TIME_PATH) as mock_time:
            mock_time.time.return_value = 0.0
            self.cooldown.reconcile([({"gpu": 1}, 10), ({"tpu": 1}, 5)])  # first sighting for both

            result = self.cooldown.reconcile([({"gpu": 1}, 1), ({"tpu": 1}, 8)])

        result_by_capset = {frozenset(caps.keys()): value for caps, value in result}
        self.assertEqual(result_by_capset[frozenset({"gpu"})], 10)  # shrinking: held
        self.assertEqual(result_by_capset[frozenset({"tpu"})], 8)  # growing: applied immediately


if __name__ == "__main__":
    unittest.main()
