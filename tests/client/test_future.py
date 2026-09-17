import math
import threading
import time
import unittest
from concurrent.futures import CancelledError, Future, InvalidStateError, TimeoutError, as_completed, wait
from threading import Event
from typing import Tuple
from unittest.mock import Mock

from scaler import Client, SchedulerClusterCombo
from scaler.client.future import ScalerFuture
from scaler.client.object_buffer import ObjectBuffer
from scaler.client.serializer.default import DefaultSerializer
from scaler.io.mixins import SyncConnector
from scaler.protocol.capnp import Task, TaskResultType
from scaler.utility.exceptions import WorkerDiedError
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.serialization import deserialize_failure
from tests.utility.utility import logging_test_name


def noop_sleep(sec: int):
    time.sleep(sec)


class TestFuture(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.address = f"tcp://127.0.0.1:{get_available_tcp_port()}"
        self._workers = 3
        self.cluster = SchedulerClusterCombo(address=self.address, n_workers=self._workers, event_loop="builtin")

    def tearDown(self) -> None:
        self.cluster.shutdown()

    def test_callback(self):
        done_called_event = Event()

        def on_done_callback(fut):
            self.assertTrue(fut.done())
            self.assertAlmostEqual(fut.result(), 4.0)
            done_called_event.set()

        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, 16.0)
            fut.add_done_callback(on_done_callback)
            done_called_event.wait()  # wait for the callback to be called, DO NOT call result().

    def test_as_completed(self):
        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, 100.0)

            for finished in as_completed([fut], timeout=5):
                self.assertAlmostEqual(finished.result(), 10.0)

    def test_state(self):
        with Client(address=self.address) as client:
            fut = client.submit(noop_sleep, 0.5)
            self.assertTrue(fut.running())
            self.assertFalse(fut.done())

            fut.result()

            self.assertFalse(fut.running())
            self.assertTrue(fut.done())

    def test_cancel(self):
        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, 100.0)
            self.assertTrue(fut.cancel())

            self.assertTrue(fut.cancelled())
            self.assertTrue(fut.done())

            with self.assertRaises(CancelledError):
                fut.result()

            fut = client.submit(math.sqrt, 16)
            fut.result()

            # cancel() should fail on a completed future.
            self.assertFalse(fut.cancel())
            self.assertFalse(fut.cancelled())

    def test_exception(self):
        with Client(address=self.address) as client:
            fut = client.submit(math.sqrt, "16")

            with self.assertRaises(TypeError):
                fut.result()

            self.assertTrue(fut.done())

            self.assertIsInstance(fut.exception(), TypeError)

    def test_client_disconnected(self):
        with Client(address=self.address) as client:
            fut = client.submit(noop_sleep, 10.0)

        with self.assertRaises(CancelledError):
            fut.result()

    def test_mocked_success(self):
        task_result = "Task result"
        client_id, future, _connector_agent, object_buffer = self.__create_mocked_future(future_result=task_result)

        # Future should be running

        self.assertTrue(future.running())
        self.assertFalse(future.done())
        self.assertFalse(future.cancelled())

        with self.assertRaises(TimeoutError):
            future.result(timeout=0)

        with self.assertRaises(TimeoutError):
            future.exception(timeout=0)

        # Future is done, but result should not have been fetched yet

        future.set_result_ready(ObjectID.generate_object_id(client_id), TaskResultType.success)

        self.assertFalse(future.running())
        self.assertTrue(future.done())
        self.assertFalse(future.cancelled())

        object_buffer.fetch_object.assert_not_called()

        # Future is done, the result should have been fetched, then deleted

        self.assertEqual(future.result(), task_result)
        self.assertIsNone(future.exception())

        object_buffer.fetch_object.assert_called_once()
        self.assertTrue(object_buffer.fetch_object.call_args.kwargs["delete_after_fetch"])

        # Cannot cancel a finished future

        self.assertFalse(future.cancel())

    def test_mocked_as_completed(self):
        """Ensure that a task result notifies as_completed waiters."""

        task_result = "Task result"
        client_id, future, _connector_agent, _object_buffer = self.__create_mocked_future(future_result=task_result)

        it = as_completed([future])

        future.set_result_ready(ObjectID.generate_object_id(client_id), TaskResultType.success)

        # The iterator should yield the finished future

        finished = next(it)
        self.assertEqual(finished.result(), task_result)

    def test_mocked_task_failure(self):
        exception = ValueError()
        client_id, future, _connector_agent, object_buffer = self.__create_mocked_future(future_result=exception)

        # Future is failing with an exception, the exception object has been fetched

        future.set_result_ready(ObjectID.generate_object_id(client_id), TaskResultType.failed)

        self.assertFalse(future.running())
        self.assertTrue(future.done())
        self.assertFalse(future.cancelled())

        object_buffer.fetch_object.assert_not_called()

        # Future is done and the exception has been fetched, then deleted

        self.assertRaises(ValueError, future.result)
        self.assertIsInstance(future.exception(), ValueError)

        object_buffer.fetch_object.assert_called_once()
        self.assertTrue(object_buffer.fetch_object.call_args.kwargs["is_exception"])

        # Cannot cancel a failed future

        self.assertFalse(future.cancel())

    def test_mocked_worker_failure(self):
        exception = WorkerDiedError()

        client_id, future, _connector_agent, object_buffer = self.__create_mocked_future(future_result=exception)

        # Future is failing with a local exception, shouldn't reach the object-storage server.

        future.set_exception(exception)

        self.assertFalse(future.running())
        self.assertTrue(future.done())
        self.assertFalse(future.cancelled())

        self.assertRaises(WorkerDiedError, future.result)
        self.assertIsInstance(future.exception(), WorkerDiedError)

        object_buffer.fetch_object.assert_not_called()

    def test_mocked_cancel(self):
        client_id, future, _connector_agent, object_buffer = self.__create_mocked_future(future_result=None)

        # Cancel the future

        # Mock the client immediately receiving the cancel confirmation after the task cancellation request
        def on_connector_send(_message):
            threading.Thread(target=future.set_canceled).start()

        _connector_agent.send.side_effect = on_connector_send

        self.assertTrue(future.cancel())

        self.assertFalse(future.running())
        self.assertTrue(future.done())
        self.assertTrue(future.cancelled())

        self.assertRaises(CancelledError, future.result)
        self.assertRaises(CancelledError, future.exception)

        # Receiving a future result from the scheduler should not change the cancelled state.
        # That might happen if the future get cancelled while the cluster is finishing the task's computation.

        with self.assertRaises(InvalidStateError):
            future.set_result_ready(ObjectID.generate_object_id(client_id), TaskResultType.success)

        object_buffer.fetch_object.assert_not_called()

    def test_mocked_cancel_concurrent(self):
        """
        Simulate a very specific cancellation case, where the client tries to cancel a future that just finished,
        but for which it hasn't received the result yet. In this scenario, `future.cancel()` returns `False` and the
        task finishes normally.
        """

        task_result = "Task result"
        client_id, future, _connector_agent, _object_buffer = self.__create_mocked_future(future_result=task_result)
        task_result_id = ObjectID.generate_object_id(client_id)

        # Cancel the finishing future

        # Mock the client receiving the task result immediately after the cancellation request
        def on_connector_send(_message):
            threading.Thread(target=lambda: future.set_result_ready(task_result_id, TaskResultType.success)).start()

        _connector_agent.send.side_effect = on_connector_send

        self.assertFalse(future.cancel())

        self.assertFalse(future.running())
        self.assertTrue(future.done())
        self.assertFalse(future.cancelled())

        self.assertEqual(future.result(), task_result)

    def test_mocked_cancel_as_completed(self):
        """Ensure that a canceled future notifies as_completed waiters."""

        client_id, future, _connector_agent, _object_buffer = self.__create_mocked_future(future_result=None)

        it = as_completed([future])

        future.set_canceled()

        # The iterator should yield the canceled future

        finished = next(it)
        self.assertTrue(finished.cancelled())

    def test_mocked_wait_does_not_return_before_every_future_is_done(self):
        """wait() must not count a future that finished before its waiter was installed.

        A delayed future that finishes while nobody is listening only moves to FINISHED; it fetches its
        result object when a waiter or callback arrives. wait() scans the batch first, so it has already
        counted that future as done -- notifying its waiter on the later fetch decrements a count that was
        never incremented for it, and wait() then returns while other futures are still running.
        """

        client_id, finished_future, _agent, _storage = self.__create_mocked_future(future_result=42)
        _client_id, pending_future, _pending_agent, _pending_storage = self.__create_mocked_future(future_result=None)

        finished_future.set_result_ready(ObjectID.generate_object_id(client_id), TaskResultType.success)
        self.assertTrue(finished_future.done())

        wait_timeout_seconds = 1.0
        start = time.time()
        done, not_done = wait([finished_future, pending_future], timeout=wait_timeout_seconds)
        elapsed = time.time() - start

        self.assertGreaterEqual(
            elapsed,
            wait_timeout_seconds * 0.9,
            "wait() returned before its timeout while one of its futures was still running",
        )
        self.assertEqual(done, {finished_future})
        self.assertEqual(not_done, {pending_future})

    @staticmethod
    def __create_mocked_future(future_result, is_delayed: bool = True) -> Tuple[ClientID, ScalerFuture, Mock, Mock]:
        client_id = ClientID.generate_client_id()
        connector_agent = Mock(spec=SyncConnector)
        object_buffer = Mock(spec=ObjectBuffer)

        def fetch_object(object_id: ObjectID, is_exception: bool, delete_after_fetch: bool) -> Future:
            # The object storage server is not available, the serialized result is returned immediately.
            payload = DefaultSerializer.serialize(future_result)

            fetched: Future = Future()
            fetched.set_result(deserialize_failure(payload) if is_exception else DefaultSerializer.deserialize(payload))
            return fetched

        object_buffer.fetch_object.side_effect = fetch_object

        task = Task(
            taskId=TaskID.generate_task_id(),
            source=client_id,
            metadata=b"",
            funcObjectId=ObjectID.generate_object_id(client_id),
            functionArgs=[],
            capabilities={},
        )

        future = ScalerFuture(
            task=task,
            is_delayed=is_delayed,
            group_task_id=None,
            serializer=DefaultSerializer(),
            connector_agent=connector_agent,
            object_buffer=object_buffer,
        )

        future.set_running_or_notify_cancel()

        return client_id, future, connector_agent, object_buffer
