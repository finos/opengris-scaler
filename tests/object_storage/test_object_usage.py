import dataclasses
import unittest

from scaler.scheduler.object_usage.object_tracker import ObjectTracker, ObjectUsage
from scaler.utility.logging.utility import setup_logger
from tests.utility.utility import logging_test_name


@dataclasses.dataclass
class Sample(ObjectUsage):
    key: str
    value: str
    size: int = 0

    def get_object_key(self) -> str:
        return self.key

    def get_object_size(self) -> int:
        return self.size


def sample_ready(obj: Sample):
    print(f"obj {obj.get_object_key()} is ready")


class TestObjectUsage(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_object_usage(self):
        object_usage: ObjectTracker[str, str, Sample] = ObjectTracker("sample", sample_ready)

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


class TestLargestObjects(unittest.TestCase):
    """`largest` walks whole size classes, so it never sorts the store to answer."""

    @staticmethod
    def tracker_holding(sizes) -> ObjectTracker[str, str, Sample]:
        tracker: ObjectTracker[str, str, Sample] = ObjectTracker("sample", sample_ready)
        for name, size in sizes.items():
            tracker.add_object(Sample(name, "value", size))
            tracker.add_blocks_for_one_object(name, {"holder"})
        return tracker

    def test_the_biggest_come_first_and_the_limit_bounds_the_list(self) -> None:
        tracker = self.tracker_holding({"small": 10, "huge": 4_000, "medium": 500, "tiny": 1})

        self.assertEqual([obj.key for obj in tracker.largest(2)], ["huge", "medium"])
        self.assertEqual([obj.key for obj in tracker.largest(10)], ["huge", "medium", "small", "tiny"])

    def test_an_object_nobody_holds_anymore_leaves_the_list(self) -> None:
        tracker = self.tracker_holding({"huge": 4_000, "medium": 500})

        tracker.remove_blocks({"holder"})

        self.assertEqual(tracker.largest(10), [])
        self.assertEqual(tracker.object_count(), 0)

    def test_the_class_the_list_ends_in_can_be_partial(self) -> None:
        """Objects within a factor of two of the smallest listed are interchangeable: taking either is the point."""
        sizes = {"huge": 3_000, "first": 1_500, "bigger": 2_000}
        tracker = self.tracker_holding(sizes)

        largest = tracker.largest(2)
        listed = {obj.key for obj in largest}
        left_out = [size for key, size in sizes.items() if key not in listed]

        self.assertEqual(largest[0].key, "huge", "a whole class above the one the list ends in is always listed")
        self.assertEqual(len(left_out), 1, "the class the list ends in is partial")
        self.assertLess(max(left_out), 2 * largest[-1].size, "nothing twice the smallest listed is left out")

    def test_an_object_added_again_is_listed_once_at_its_new_size(self) -> None:
        tracker = self.tracker_holding({"resent": 10})

        tracker.add_object(Sample("resent", "value", 4_000))

        self.assertEqual([(obj.key, obj.size) for obj in tracker.largest(10)], [("resent", 4_000)])
