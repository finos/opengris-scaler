import sys
import unittest
from unittest import mock

from scaler.config.common.worker import WorkerConfig


class TestWorkerConfig(unittest.TestCase):
    def test_hard_processor_suspend_rejected_on_windows(self):
        """Hard processor suspend uses POSIX SIGSTOP/SIGCONT and has no Windows equivalent.
        Surface this as a config-time error rather than failing deep inside the processor.
        """
        with mock.patch.object(sys, "platform", "win32"):
            with self.assertRaises(ValueError) as ctx:
                WorkerConfig(hard_processor_suspend=True)
            self.assertIn("hard_processor_suspend", str(ctx.exception))
            self.assertIn("Windows", str(ctx.exception))

    def test_hard_processor_suspend_allowed_on_posix(self):
        with mock.patch.object(sys, "platform", "linux"):
            cfg = WorkerConfig(hard_processor_suspend=True)
            self.assertTrue(cfg.hard_processor_suspend)

    def test_default_hard_processor_suspend_is_off(self):
        with mock.patch.object(sys, "platform", "win32"):
            cfg = WorkerConfig()
            self.assertFalse(cfg.hard_processor_suspend)


if __name__ == "__main__":
    unittest.main()
