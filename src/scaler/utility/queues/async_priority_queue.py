from asyncio import Queue
from typing import Any, Dict, List, Tuple, Union

from sortedcontainers import SortedDict

PriorityType = Union[int, Tuple["PriorityType", ...]]


class AsyncPriorityQueue(Queue):
    """A subclass of Queue; retrieves entries in priority order (lowest first).

    Input entries are typically list of the form: [priority, data].
    """

    def __len__(self):
        return len(self._queue)

    def _init(self, maxsize):
        # self._locator contains {data : List[[priority, data], count]}
        # self._queue contains {(priority, count) : data}
        self._locator: Dict[bytes, List] = {}
        self._queue = SortedDict()
        self._item_counter: int = 0

    def _put(self, item):
        if not isinstance(item, list):
            item = list(item)

        self._locator[item[1]] = [item, self._item_counter]
        self._queue[(item[0], self._item_counter)] = item[1]
        self._item_counter += 1

    def _get(self):
        (priority, _), data = self._queue.popitem(0)
        self._locator.pop(data)
        return priority, data

    def remove(self, data):
        item, count = self._locator.pop(data)
        self._queue.pop((item[0], count))

    def decrease_priority(self, data):
        """Decrease the priority *value* of an item in the queue, effectively move data closer to the front.

        Notes:
            - *priority* in the signature means the priority *value* of the item.
            - Time complexity is O(log n) due to the underlying SortedDict structure.
        """
        item, count = self._locator[data]
        self._locator[data][1] = self._item_counter
        self._queue.pop((item[0], count))

        item[0] -= 1
        self._queue[(item[0], self._item_counter)] = item[1]
        self._item_counter += 1

    def max_priority_item(self) -> Tuple[PriorityType, Any]:
        """Return the current item at the front of the queue without removing it from the queue.

        Notes:
            - This is a "peek" operation; it does not modify the queue.
            - For items with the same priority, insertion order determines which item is returned first.
            - *priority* means the priority in the queue
            - Time complexity is O(1) as we are peeking in the head
        """
        (priority, _), data = self._queue.peekitem(0)
        return (priority, data)
