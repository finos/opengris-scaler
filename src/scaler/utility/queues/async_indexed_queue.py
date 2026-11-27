import sys
from asyncio import Queue, QueueEmpty
from typing import TypeVar

from scaler.utility.queues.indexed_queue import IndexedQueue

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

ItemType = TypeVar("ItemType")


class AsyncIndexedQueue(Queue[ItemType]):
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

    @override
    def _init(self, maxsize: int):
        self._queue: IndexedQueue[ItemType] = IndexedQueue()

    @override
    def _put(self, item: ItemType):
        self._queue.put(item)

    @override
    def _get(self):
        try:
            return self._queue.get()
        except IndexError:
            raise QueueEmpty(f"{self.__class__.__name__} queue empty")

    def remove(self, item: ItemType):
        """remove the item in the queue in O(1) time complexity"""
        self._queue.remove(item)
