import asyncio
import unittest

from scaler.utility.logging.utility import setup_logger
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from tests.utility.utility import logging_test_name


class TestAsyncPriorityQueue(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_async_priority_queue_basic(self):
        async def async_test():
            queue = AsyncPriorityQueue()
            await queue.put((5, 5))
            await queue.put((2, 2))
            await queue.put((6, 6))
            await queue.put((-3, 0))  # supports negative priorities
            await queue.put((1, 1))
            await queue.put((4, 4))
            await queue.put((3, 3))

            queue.remove(2)
            queue.remove(3)
            self.assertEqual(queue.qsize(), 5)

            queue.decrease_priority(4)  # (4, 4) becomes (3, 4)

            self.assertEqual(await queue.get(), (-3, 0))
            self.assertEqual(await queue.get(), (1, 1))
            self.assertEqual(await queue.get(), (3, 4))
            self.assertEqual(await queue.get(), (5, 5))
            self.assertEqual(await queue.get(), (6, 6))
            self.assertEqual(queue.qsize(), 0)
            self.assertTrue(not queue)
            self.assertTrue(queue.empty())

        asyncio.run(async_test())

    def test_stable_insertion(self):
        async def async_test():
            queue = AsyncPriorityQueue()

            await queue.put((1, 4))
            await queue.put((1, 3))
            await queue.put((1, 2))

            # Stability: insertion order preserved
            self.assertEqual(await queue.get(), (1, 4))
            self.assertEqual(await queue.get(), (1, 3))
            self.assertEqual(await queue.get(), (1, 2))

        asyncio.run(async_test())

    def test_decrease_priority_reorders_correctly(self):
        async def async_test():
            queue = AsyncPriorityQueue()

            await queue.put((5, "x"))
            await queue.put((1, "y"))
            await queue.put((3, "z"))

            queue.decrease_priority("x")
            queue.decrease_priority("x")
            queue.decrease_priority("x")
            # "x" has priority 2 after decrease

            self.assertEqual(await queue.get(), (1, "y"))
            self.assertEqual(await queue.get(), (2, "x"))
            self.assertEqual(await queue.get(), (3, "z"))

        asyncio.run(async_test())

    def test_remove(self):
        async def async_test():
            queue = AsyncPriorityQueue()

            await queue.put((1, "a"))
            await queue.put((2, "b"))
            await queue.put((3, "c"))
            await queue.put((4, "d"))

            queue.remove("b")
            queue.remove("d")

            self.assertEqual(queue.qsize(), 2)
            self.assertEqual(await queue.get(), (1, "a"))
            self.assertEqual(await queue.get(), (3, "c"))
            self.assertTrue(queue.empty())

        asyncio.run(async_test())

    def test_max_priority_item(self):
        async def async_test():
            queue = AsyncPriorityQueue()

            await queue.put((10, "low"))
            await queue.put((1, "high"))
            await queue.put((5, "mid"))

            priority, data = queue.max_priority_item()
            self.assertEqual(priority, 1)
            self.assertEqual(data, "high")

            # Ensure peek does not remove
            self.assertEqual(queue.qsize(), 3)

        asyncio.run(async_test())

    def test_interleaved_put_get(self):
        async def async_test():
            queue = AsyncPriorityQueue()

            await queue.put((2, "b"))
            self.assertEqual(await queue.get(), (2, "b"))

            await queue.put((3, "c"))
            await queue.put((1, "a"))

            self.assertEqual(await queue.get(), (1, "a"))

            await queue.put((0, "z"))
            self.assertEqual(await queue.get(), (0, "z"))
            self.assertEqual(await queue.get(), (3, "c"))

            self.assertTrue(queue.empty())

        asyncio.run(async_test())

    def test_len(self):
        async def async_test():
            queue = AsyncPriorityQueue()

            self.assertEqual(len(queue), 0)
            self.assertEqual(queue.qsize(), 0)

            await queue.put((1, 1))
            await queue.put((2, 2))

            self.assertEqual(len(queue), 2)
            self.assertEqual(queue.qsize(), 2)

            await queue.get()

            self.assertEqual(len(queue), 1)
            self.assertEqual(queue.qsize(), 1)

        asyncio.run(async_test())
