import unittest

from scaler.utility.logging.utility import setup_logger
from scaler.utility.queues.indexed_queue import IndexedQueue
from tests.utility.utility import logging_test_name


class TestIndexedQueue(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_indexed_queue(self):
        queue = IndexedQueue()
        queue.put(1)
        queue.put(2)
        queue.put(3)
        queue.put(4)
        queue.put(5)
        queue.put(6)

        self.assertEqual(len(queue), 6)

        self.assertTrue(1 in queue)
        self.assertTrue(0 not in queue)

        queue.remove(3)
        self.assertEqual(len(queue), 5)
        self.assertTrue(3 not in queue)

        self.assertListEqual(queue.to_list(), [1, 2, 4, 5, 6])

        self.assertEqual(queue.get(), 1)
        self.assertEqual(queue.get(), 2)
        self.assertEqual(len(queue), 3)

    def test_to_list_empty(self):
        queue = IndexedQueue()
        self.assertListEqual(queue.to_list(), [])

    def test_to_list_single(self):
        queue = IndexedQueue()
        queue.put(42)
        self.assertListEqual(queue.to_list(), [42])

    def test_to_list_order_after_get(self):
        queue = IndexedQueue()
        queue.put(1)
        queue.put(2)
        queue.put(3)
        queue.get()
        self.assertListEqual(queue.to_list(), [2, 3])

    def test_to_list_multiple_times(self):
        queue = IndexedQueue()
        queue.put("a")
        queue.put("b")
        queue.put("c")
        self.assertListEqual(queue.to_list(), ["a", "b", "c"])
        self.assertListEqual(queue.to_list(), ["a", "b", "c"])

    def test_to_list_not_iterable(self):
        queue = IndexedQueue()
        queue.put(1)
        with self.assertRaises(TypeError):
            list(queue)

        with self.assertRaises(TypeError):
            for _ in queue:
                pass
