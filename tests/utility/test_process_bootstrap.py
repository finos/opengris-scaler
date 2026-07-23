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

    def _mock_resolved_log_path(self, log_path: str) -> None:
        mock.patch("scaler.utility.process_bootstrap.get_logger_info", return_value=("", "INFO", (log_path,))).start()

    def test_dash_sentinel_targets_stdout(self) -> None:
        self._mock_resolved_log_path("-")

        bootstrap_process()

        self.mock_enable.assert_called_once_with(file=sys.stdout, all_threads=True)

    def test_dev_stdout_sentinel_targets_stdout(self) -> None:
        self._mock_resolved_log_path("/dev/stdout")

        bootstrap_process()

        self.mock_enable.assert_called_once_with(file=sys.stdout, all_threads=True)

    def test_dev_stderr_sentinel_targets_stderr(self) -> None:
        self._mock_resolved_log_path("/dev/stderr")

        bootstrap_process()

        self.mock_enable.assert_called_once_with(file=sys.stderr, all_threads=True)

    def test_file_path_opens_that_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = os.path.join(tmp_dir, "crash.log")
            self._mock_resolved_log_path(log_path)

            bootstrap_process()

            self.mock_enable.assert_called_once()
            _, kwargs = self.mock_enable.call_args
            self.assertEqual(kwargs["file"].name, log_path)
            self.assertTrue(kwargs["all_threads"])
            kwargs["file"].close()

    def test_unopenable_path_falls_back_to_stderr(self) -> None:
        self._mock_resolved_log_path("/nonexistent-directory-xyz/crash.log")

        bootstrap_process()

        self.mock_enable.assert_called_once_with(all_threads=True)

    def test_second_call_closes_previous_file_handle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            first_log_path = os.path.join(tmp_dir, "first.log")
            self._mock_resolved_log_path(first_log_path)
            bootstrap_process()
            first_file = self.mock_enable.call_args.kwargs["file"]

            second_log_path = os.path.join(tmp_dir, "second.log")
            self._mock_resolved_log_path(second_log_path)
            bootstrap_process()
            second_file = self.mock_enable.call_args.kwargs["file"]

            self.assertTrue(first_file.closed, "the first faulthandler file handle was not closed")
            second_file.close()

    def test_logging_config_file_derives_target_from_configured_logger_not_log_paths(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            log_path = os.path.join(tmp_dir, "from_config_file.log")
            self._mock_resolved_log_path(log_path)

            # log_paths points at stdout, but the resolved logger state must win regardless.
            bootstrap_process(logging_config_file="unused.ini", log_paths=("/dev/stdout",))

            self.mock_enable.assert_called_once()
            _, kwargs = self.mock_enable.call_args
            self.assertEqual(kwargs["file"].name, log_path)
            kwargs["file"].close()
