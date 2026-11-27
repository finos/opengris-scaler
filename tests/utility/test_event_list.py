import unittest

from scaler.utility.event_list import EventList


class TestEventList(unittest.TestCase):
    def test_adding_returns_a_new_list_and_leaves_both_operands_unchanged(self) -> None:
        updates = []
        events = EventList([1, 2])
        events.add_update_callback(updates.append)

        combined = events + [3]

        self.assertEqual(combined, EventList([1, 2, 3]))
        self.assertEqual(events, EventList([1, 2]))
        self.assertEqual(updates, [])

    def test_appending_reports_the_update(self) -> None:
        updates = []
        events = EventList([1])
        events.add_update_callback(updates.append)

        events.append(2)

        self.assertEqual(updates, [events])
