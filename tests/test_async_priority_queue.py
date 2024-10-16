import asyncio
import unittest

from scaler.utility.logging.utility import setup_logger
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from tests.utility import logging_test_name


class TestAsyncPriorityQueue(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_async_priority_queue(self):
        async def async_test():
            queue = AsyncPriorityQueue()
            await queue.put((5, 5))
            await queue.put((2, 2))
            await queue.put((6, 6))
            await queue.put((1, 1))
            await queue.put((4, 4))
            await queue.put((3, 3))

            queue.remove(2)
            queue.remove(3)
            self.assertEqual(queue.qsize(), 4)

            queue.decrease_priority(4)  # (4, 4) becomes (3, 4)

            self.assertEqual(await queue.get(), (1, 1))
            self.assertEqual(await queue.get(), (3, 4))
            self.assertEqual(await queue.get(), (5, 5))
            self.assertEqual(await queue.get(), (6, 6))
            self.assertEqual(queue.qsize(), 0)
            self.assertTrue(not queue)
            self.assertTrue(queue.empty())

        asyncio.run(async_test())
