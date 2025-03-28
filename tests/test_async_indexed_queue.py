import asyncio
import unittest

from scaler.utility.logging.utility import setup_logger
from scaler.utility.queues.async_indexed_queue import AsyncIndexedQueue
from tests.utility import logging_test_name


class TestAsyncIndexedQueue(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_async_indexed_queue(self):
        async def async_test():
            queue = AsyncIndexedQueue()
            await queue.put(1)
            await queue.put(2)
            await queue.put(3)
            await queue.put(4)
            await queue.put(5)
            await queue.put(6)

            queue.remove(1)
            queue.remove(3)
            queue.remove(6)
            self.assertEqual(queue.qsize(), 3)

            self.assertEqual(await queue.get(), 2)
            self.assertEqual(await queue.get(), 4)
            self.assertEqual(await queue.get(), 5)
            self.assertEqual(queue.qsize(), 0)
            self.assertTrue(not queue)
            self.assertTrue(queue.empty())

        asyncio.run(async_test())

    def test_duplicated_items(self):
        async def async_test():
            queue = AsyncIndexedQueue(3)
            await queue.put(1)
            await queue.put(1)

        with self.assertRaises(KeyError):
            asyncio.run(async_test())
