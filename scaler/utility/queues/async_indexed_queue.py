from asyncio import Queue, QueueEmpty
from typing import Generic, TypeVar

from scaler.utility.queues.indexed_queue import IndexedQueue

ItemType = TypeVar("ItemType")


class AsyncIndexedQueue(Queue, Generic[ItemType]):
    """This should have same set of features as asyncio.Queue, with additional methods like remove
    - it behaves like regular async queue, except:
      - all the items pushed to queue should be hashable
      - those items should be unique in queue
    - IndexedQueue.put(), IndexedQueue.get(), IndexedQueue.remove() should all take O(1) time complexity
    """

    def __contains__(self, item: ItemType):
        return item in self._queue

    def __len__(self):
        return self._queue.__len__()

    def _init(self, maxsize):
        self._queue = IndexedQueue()

    def _put(self, item: ItemType):
        self._queue.put(item)

    def _get(self):
        try:
            return self._queue.get()
        except IndexError:
            raise QueueEmpty(f"{self.__class__.__name__} queue empty")

    def remove(self, item: ItemType):
        """remove the item in the queue in O(1) time complexity"""
        self._queue.remove(item)
