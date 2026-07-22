import os
import sys
import tempfile
import unittest
from unittest import mock

from scaler.utility.process_bootstrap import bootstrap_process


class TestBootstrapProcessEnablesFaulthandler(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_enable = mock.patch("scaler.utility.process_bootstrap.faulthandler.enable").start()
        mock.patch("scaler.utility.process_bootstrap.setup_logger").start()
        self.addCleanup(mock.patch.stopall)

    def test_dash_sentinel_targets_stdout(self) -> None:
        bootstrap_process(log_paths=("-",))

        self.mock_enable.assert_called_once_with(file=sys.stdout, all_threads=True)

    def test_dev_stdout_sentinel_targets_stdout(self) -> None:
        bootstrap_process(log_paths=("/dev/stdout",))

        self.mock_enable.assert_called_once_with(file=sys.stdout, all_threads=True)

    def test_file_path_opens_that_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = os.path.join(tmp_dir, "crash.log")

            bootstrap_process(log_paths=(log_path,))

            self.mock_enable.assert_called_once()
            _, kwargs = self.mock_enable.call_args
            self.assertEqual(kwargs["file"].name, log_path)
            self.assertTrue(kwargs["all_threads"])
            kwargs["file"].close()

    def test_unopenable_path_falls_back_to_stderr(self) -> None:
        bootstrap_process(log_paths=("/nonexistent-directory-xyz/crash.log",))

        self.mock_enable.assert_called_once_with(all_threads=True)

    def test_no_log_paths_falls_back_to_stderr(self) -> None:
        bootstrap_process(log_paths=())

        self.mock_enable.assert_called_once_with(all_threads=True)


if __name__ == "__main__":
    unittest.main()
