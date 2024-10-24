import unittest

from scaler.protocol.python.common import ObjectContent
from scaler.protocol.python.message import (ObjectInstruction, ObjectRequest,
                                            ObjectResponse)
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.object_tracker import VanillaObjectTracker
from tests.utility import logging_test_name


class TestWorkerObjectTracker(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_object_tracker(self) -> None:
        tracker = VanillaObjectTracker()

        tracker.on_object_request(
            b"processor_1", ObjectRequest.new_msg(ObjectRequest.ObjectRequestType.Get, (b"object_1", b"object_2"))
        )

        tracker.on_object_request(
            b"processor_2", ObjectRequest.new_msg(ObjectRequest.ObjectRequestType.Get, (b"object_1", b"object_2"))
        )
        tracker.on_object_request(
            b"processor_2", ObjectRequest.new_msg(ObjectRequest.ObjectRequestType.Get, (b"object_3",))
        )

        tracker.on_object_request(
            b"processor_3", ObjectRequest.new_msg(ObjectRequest.ObjectRequestType.Get, (b"object_4", b"object_5"))
        )

        response_1 = tracker.on_object_response(
            ObjectResponse.new_msg(
                ObjectResponse.ObjectResponseType.Content, ObjectContent.new_msg((b"object_1", b"object_2"))
            )
        )
        self.assertSetEqual(set(response_1), {b"processor_1", b"processor_2"})

        response_2 = tracker.on_object_response(
            ObjectResponse.new_msg(
                ObjectResponse.ObjectResponseType.Content, ObjectContent.new_msg((b"object_unknown",))
            )
        )
        self.assertSetEqual(response_2, set())

        response_3 = tracker.on_object_response(
            ObjectResponse.new_msg(ObjectResponse.ObjectResponseType.Content, ObjectContent.new_msg((b"object_3",)))
        )
        self.assertSetEqual(response_3, {b"processor_2"})

        object_instructions = tracker.on_object_instruction(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Delete,
                b"client",
                ObjectContent.new_msg(
                    (b"object_1", b"object_2", b"object_3"),
                    (b"name_1", b"name_2", b"name_3"),
                    ([b"content_1"], [b"content_2"], [b"content_3"]),
                ),
            )
        )
        self.assertSetEqual(set(object_instructions.keys()), {b"processor_1", b"processor_2"})

        tracker.on_processor_end(b"processor_3")

        self.assertEqual(len(tracker._object_request_to_processor_ids.left_keys()), 0)
        self.assertEqual(len(tracker._object_id_to_processors_ids.left_keys()), 0)
