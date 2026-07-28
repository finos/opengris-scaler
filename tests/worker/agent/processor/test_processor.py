import logging
import unittest
from typing import Any, Optional
from unittest.mock import Mock

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import Task, TaskResultType
from scaler.utility.exceptions import ObjectStorageException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.worker.agent.processor.processor import Processor

_LOGGER_NAME = "scaler.worker.agent.processor.processor"


def _make_processor() -> Processor:
    # The processor is never started: these tests exercise the pure logic of __log_exit and __send_result with
    # the connectors mocked out, so nothing here needs a live child process.
    address = AddressConfig.from_string("tcp://127.0.0.1:0")
    return Processor(
        event_loop="builtin",
        agent_address=address,
        scheduler_address=address,
        object_storage_address=address,
        preload=None,
        resume_event=None,
        resumed_event=None,
        suspend_trigger=None,
        garbage_collect_interval_seconds=60,
        trim_memory_threshold_bytes=10**9,
        logging_paths=("/dev/stdout",),
        logging_level="INFO",
    )


def _make_task() -> Task:
    return Task(
        taskId=TaskID.generate_task_id(),
        source=ClientID.generate_client_id("test"),
        metadata=b"",
        funcObjectId=ObjectID(b"\x00" * 16 + b"\x01" * 16),
        functionArgs=[],
    )


class ProcessorLogExitTest(unittest.TestCase):
    """__log_exit is the processor's only report of why its main loop stopped, so it must name the orphaned task,
    keep the traceback for genuine faults, and stay quiet for a teardown the agent itself requested."""

    def setUp(self) -> None:
        self.processor = _make_processor()

    def __log_exit(self, reason: str, exception: Optional[BaseException] = None) -> logging.LogRecord:
        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG) as captured:
            self.processor._Processor__log_exit(reason, exception=exception)  # type: ignore[attr-defined]

        self.assertEqual(len(captured.records), 1)
        return captured.records[0]

    def test_idle_exit_is_debug_without_traceback(self) -> None:
        record = self.__log_exit("agent connector stop requested")

        self.assertEqual(record.levelno, logging.DEBUG)
        self.assertIsNone(record.exc_info)
        self.assertIn("agent connector stop requested", record.getMessage())

    def test_exception_is_error_with_traceback(self) -> None:
        exception = ObjectStorageException("storage went away")

        record = self.__log_exit("object storage error", exception=exception)

        self.assertEqual(record.levelno, logging.ERROR)
        self.assertIsNotNone(record.exc_info)
        self.assertIs(record.exc_info[1], exception)  # type: ignore[index]

    def test_in_flight_task_is_named_when_an_exception_is_present(self) -> None:
        # Regression: __send_result used to clear _current_task before writing to storage, so the PR's headline
        # scenario -- a storage failure mid-result -- logged without saying which task was orphaned.
        task = _make_task()
        self.processor._current_task = task

        record = self.__log_exit("object storage error", exception=ObjectStorageException("boom"))

        message = record.getMessage()
        self.assertEqual(record.levelno, logging.ERROR)
        self.assertIn(task.taskId.hex(), message)
        self.assertIn("no task result will be sent", message)

    def test_in_flight_task_without_exception_is_warning(self) -> None:
        task = _make_task()
        self.processor._current_task = task

        record = self.__log_exit("interrupted")

        message = record.getMessage()
        self.assertEqual(record.levelno, logging.WARNING)
        self.assertIsNone(record.exc_info)
        self.assertIn(task.taskId.hex(), message)
        self.assertIn("no task result will be sent", message)

    def test_interrupted_exit_is_quiet_even_with_an_exception(self) -> None:
        # A deliberate kill sends SIGTERM, whose handler destroys the connectors on purpose; a processor mid
        # storage call then raises "connector is closed." That is expected teardown, not an error, so it must
        # not be reported as ERROR with a traceback for something the agent asked for.
        self.processor._current_task = _make_task()
        self.processor._interrupted = True

        record = self.__log_exit("object storage error", exception=ObjectStorageException("closed"))

        self.assertEqual(record.levelno, logging.DEBUG)
        self.assertIsNone(record.exc_info)
        self.assertIn("stopped on agent request", record.getMessage())

    def test_interrupt_sets_the_interrupted_flag(self) -> None:
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()

        self.assertFalse(self.processor._interrupted)

        self.processor._Processor__interrupt()  # type: ignore[attr-defined]

        self.assertTrue(self.processor._interrupted)
        self.processor._connector_agent.destroy.assert_called_once()
        self.processor._connector_storage.destroy.assert_called_once()


class ProcessorSendResultTest(unittest.TestCase):
    """_current_task must stay set until the result is fully handed off, so a failure part-way through can still
    report which task was orphaned."""

    def setUp(self) -> None:
        self.processor = _make_processor()
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()

        self.task = _make_task()
        self.processor._current_task = self.task

    def __send_result(self) -> None:
        self.processor._Processor__send_result(  # type: ignore[attr-defined]
            self.task.source, self.task.taskId, TaskResultType.success, b"result-bytes"
        )

    def test_current_task_is_cleared_once_the_result_is_sent(self) -> None:
        self.__send_result()

        self.assertIsNone(self.processor._current_task)

    def test_current_task_survives_a_storage_failure(self) -> None:
        storage: Any = self.processor._connector_storage
        storage.set_object.side_effect = ObjectStorageException("storage went away")

        with self.assertRaises(ObjectStorageException):
            self.__send_result()

        self.assertIs(self.processor._current_task, self.task)


class ProcessorRunForeverTest(unittest.TestCase):
    """SystemExit must reach multiprocessing's _bootstrap, otherwise the processor exits 0 and the exit code
    this PR surfaces is destroyed."""

    def setUp(self) -> None:
        self.processor = _make_processor()
        self.processor._connector_agent = Mock()
        self.processor._connector_storage = Mock()
        self.processor._object_cache = Mock()

    def test_system_exit_is_logged_and_re_raised(self) -> None:
        # A task calling sys.exit(3) raises SystemExit, which __process_task's `except Exception` does not
        # catch, so it unwinds into __run_forever.
        agent: Any = self.processor._connector_agent
        agent.receive.side_effect = SystemExit(3)

        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG):
            with self.assertRaises(SystemExit) as context:
                self.processor._Processor__run_forever()  # type: ignore[attr-defined]

        self.assertEqual(context.exception.code, 3)

    def test_storage_error_does_not_escape_the_loop(self) -> None:
        agent: Any = self.processor._connector_agent
        agent.receive.side_effect = ObjectStorageException("storage went away")

        with self.assertLogs(_LOGGER_NAME, level=logging.DEBUG) as captured:
            self.processor._Processor__run_forever()  # type: ignore[attr-defined]

        self.assertEqual(captured.records[0].levelno, logging.ERROR)


if __name__ == "__main__":
    unittest.main()
