import threading
import unittest
from typing import Tuple
from unittest.mock import Mock

from scaler.client.agent.future_manager import ClientFutureManager
from scaler.client.future import ScalerFuture
from scaler.client.object_buffer import ObjectBuffer
from scaler.client.serializer.default import DefaultSerializer
from scaler.io.mixins import SyncConnector
from scaler.protocol.capnp import Task
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name

CANCEL_BUDGET_SECONDS = 0.5
JOIN_TIMEOUT_SECONDS = 10.0


class TestClientFutureManager(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_cancel_all_futures_gives_up_on_a_cancel_that_is_never_confirmed(self):
        """disconnect() must not hang on a cancel the scheduler never answers."""

        manager = ClientFutureManager(DefaultSerializer())
        _client_id, future = self.__create_future_nobody_will_answer()
        manager.add_future(future)

        # Run on a thread so a regression fails the test rather than hanging the whole suite.
        done = threading.Event()

        def cancel_all() -> None:
            manager.cancel_all_futures(CANCEL_BUDGET_SECONDS)
            done.set()

        thread = threading.Thread(target=cancel_all, daemon=True)
        thread.start()

        self.assertTrue(
            done.wait(JOIN_TIMEOUT_SECONDS), "cancel_all_futures did not return; an unanswered cancel blocks forever"
        )
        thread.join(JOIN_TIMEOUT_SECONDS)

        # The client is leaving, so the future settles either way rather than being left pending.
        self.assertTrue(future.cancelled())

    @staticmethod
    def __create_future_nobody_will_answer() -> Tuple[ClientID, ScalerFuture]:
        client_id = ClientID.generate_client_id()
        connector_agent = Mock(spec=SyncConnector)  # accepts the TaskCancel and never answers it
        object_buffer = Mock(spec=ObjectBuffer)

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
            is_delayed=True,
            group_task_id=None,
            serializer=DefaultSerializer(),
            connector_agent=connector_agent,
            object_buffer=object_buffer,
        )

        return client_id, future


if __name__ == "__main__":
    unittest.main()
