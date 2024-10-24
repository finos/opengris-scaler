import asyncio
import unittest

from scaler.utility.logging.utility import setup_logger
from scaler.utility.queues.async_sorted_priority_queue import \
    AsyncSortedPriorityQueue
from tests.utility import logging_test_name


class TestSortedPriorityQueue(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_sorted_priority_queue(self):
        async def async_test():
            queue = AsyncSortedPriorityQueue()

            await queue.put([2, 3])
            await queue.put([3, 5])
            await queue.put([1, 1])
            await queue.put([3, 6])
            await queue.put([2, 4])
            await queue.put([1, 2])

            queue.remove(4)
            self.assertEqual(queue.qsize(), 5)

            self.assertEqual(await queue.get(), [1, 1])
            self.assertEqual(await queue.get(), [1, 2])
            self.assertEqual(await queue.get(), [2, 3])
            self.assertEqual(await queue.get(), [3, 5])
            self.assertEqual(await queue.get(), [3, 6])
            self.assertEqual(queue.qsize(), 0)
            self.assertTrue(not queue)
            self.assertTrue(queue.empty())

        asyncio.run(async_test())
