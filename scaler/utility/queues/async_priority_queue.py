import heapq
from asyncio import Queue
from typing import Dict, List, Tuple, Union

PriorityType = Union[int, Tuple["PriorityType", ...]]


class AsyncPriorityQueue(Queue):
    """A subclass of Queue; retrieves entries in priority order (lowest first).

    Entries are typically list of the form: [priority, data].
    """

    def __len__(self):
        return len(self._queue)

    def _init(self, maxsize):
        self._queue: List[List] = []
        self._locator: Dict[bytes, List] = {}

    def _put(self, item):
        if not isinstance(item, list):
            item = list(item)

        heapq.heappush(self._queue, item)
        self._locator[item[1]] = item

    def _get(self):
        priority, data = heapq.heappop(self._queue)
        self._locator.pop(data)
        return priority, data

    def remove(self, data):
        # this operation is O(n), first change priority to -1 and pop from top of the heap, mark it as invalid
        # entry in the heap is not good idea as those invalid, entry will never get removed, so we used heapq internal
        # function _siftdown to maintain min heap invariant
        item = self._locator.pop(data)
        i = self._queue.index(item)  # O(n)
        item[0] = self.__to_lowest_priority(item[0])
        heapq._siftdown(self._queue, 0, i)  # type: ignore[attr-defined]
        assert heapq.heappop(self._queue) == item

    def decrease_priority(self, data):
        # this operation should be O(n), mark it as invalid entry in the heap is not good idea as those invalid
        # entry will never get removed, so we used heapq internal function _siftdown to maintain min heap invariant
        item = self._locator[data]
        i = self._queue.index(item)  # O(n)
        item[0] = self.__to_lower_priority(item[0])
        heapq._siftdown(self._queue, 0, i)  # type: ignore[attr-defined]

    def max_priority(self):
        item = heapq.heappop(self._queue)
        heapq.heappush(self._queue, item)
        priority = item[0]
        return priority

    @classmethod
    def __to_lowest_priority(cls, original_priority: PriorityType) -> PriorityType:
        if isinstance(original_priority, tuple):
            return tuple(cls.__to_lowest_priority(value) for value in original_priority)
        else:
            return -1

    @classmethod
    def __to_lower_priority(cls, original_priority: PriorityType) -> PriorityType:
        if isinstance(original_priority, tuple):
            return tuple(cls.__to_lower_priority(value) for value in original_priority)
        else:
            return original_priority - 1
