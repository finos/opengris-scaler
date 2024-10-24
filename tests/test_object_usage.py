import dataclasses
import unittest

from scaler.scheduler.object_usage.object_tracker import (ObjectTracker,
                                                          ObjectUsage)
from scaler.utility.logging.utility import setup_logger
from tests.utility import logging_test_name


@dataclasses.dataclass
class Sample(ObjectUsage):
    key: str
    value: str

    def get_object_key(self) -> str:
        return self.key


def sample_ready(obj: Sample):
    print(f"obj {obj.get_object_key()} is ready")


class TestObjectUsage(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_object_usage(self):
        object_usage: ObjectTracker[str, Sample] = ObjectTracker("sample", sample_ready)

        object_usage.add_object(Sample("a", "value1"))
        object_usage.add_object(Sample("b", "value2"))

        object_usage.add_blocks_for_one_object("a", {"1", "2"})
        object_usage.add_blocks_for_one_object("b", {"2", "3", "4"})

        object_usage.remove_blocks({"1"})
        self.assertEqual(object_usage.object_count(), 2)

        object_usage.remove_blocks({"3"})
        self.assertEqual(object_usage.object_count(), 2)

        object_usage.remove_blocks({"2"})
        self.assertEqual(object_usage.object_count(), 1)

        object_usage.remove_blocks({"4"})
        self.assertEqual(object_usage.object_count(), 0)

        object_usage.remove_blocks({"4"})
