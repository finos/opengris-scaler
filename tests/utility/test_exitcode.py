import sys
import unittest

from scaler.utility.exitcode import describe_exitcode


class TestDescribeExitcode(unittest.TestCase):
    def test_clean_exit_is_the_number(self) -> None:
        self.assertEqual(describe_exitcode(0), "0")
        self.assertEqual(describe_exitcode(1), "1")

    @unittest.skipIf(sys.platform == "win32", "negative signal exit codes and their names are POSIX-specific")
    def test_signal_death_is_named(self) -> None:
        self.assertEqual(describe_exitcode(-9), "-9 (SIGKILL)")  # e.g. OOM kill
        self.assertEqual(describe_exitcode(-15), "-15 (SIGTERM)")
        self.assertEqual(describe_exitcode(-6), "-6 (SIGABRT)")

    def test_unknown_signal_falls_back_to_number(self) -> None:
        self.assertEqual(describe_exitcode(-999), "-999")

    def test_none_when_not_exited(self) -> None:
        self.assertEqual(describe_exitcode(None), "None")
