from asyncio import Queue
from dataclasses import dataclass
from typing import Any, Dict, Tuple, Union

from sortedcontainers import SortedDict

PriorityType = Union[int, Tuple["PriorityType", ...]]


class AsyncPriorityQueue(Queue):
    """A subclass of Queue; retrieves entries in priority order (lowest first).

    Input entries are typically list of the form: [priority, data].
    """

    @dataclass(frozen=True)
    class MapKey:
        priority: int
        count: int

        def __lt__(self, other):
            return (self.priority, self.count) < (other.priority, other.count)

        def __hash__(self):
            return hash((self.priority, self.count))

    @dataclass
    class LocatorValue:
        map_key: "AsyncPriorityQueue.MapKey"
        data: bytes

    def __len__(self):
        return len(self._queue)

    def _init(self, maxsize):
        self._locator: Dict[bytes, AsyncPriorityQueue.LocatorValue] = {}
        self._queue: Dict[AsyncPriorityQueue.MapKey, bytes] = SortedDict()
        self._item_counter: int = 0

    def _put(self, item):
        if not isinstance(item, list):
            item = list(item)

        priority, data = item
        map_key = AsyncPriorityQueue.MapKey(priority=priority, count=self._item_counter)
        self._locator[data] = AsyncPriorityQueue.LocatorValue(map_key=map_key, data=data)
        self._queue[map_key] = data
        self._item_counter += 1

    def _get(self):
        map_key, data = self._queue.popitem(0)  # type: ignore[call-arg]
        self._locator.pop(data)
        return map_key.priority, data

    def remove(self, data):
        loc_value = self._locator.pop(data)
        self._queue.pop(loc_value.map_key)

    def decrease_priority(self, data):
        # Decrease the priority *value* of an item in the queue, effectively move data closer to the front.
        # Notes:
        #     - *priority* in the signature means the priority *value* of the item.
        #     - Time complexity is O(log n) due to the underlying SortedDict structure.

        loc_value = self._locator[data]
        map_key = AsyncPriorityQueue.MapKey(priority=loc_value.map_key.priority - 1, count=self._item_counter)
        new_loc_value = AsyncPriorityQueue.LocatorValue(map_key=map_key, data=data)
        self._locator[data] = new_loc_value
        self._queue.pop(loc_value.map_key)
        self._queue[map_key] = data
        self._item_counter += 1

    def max_priority_item(self) -> Tuple[PriorityType, Any]:
        """Return the current item at the front of the queue without removing it from the queue.

        Notes:
            - This is a "peek" operation; it does not modify the queue.
            - For items with the same priority, insertion order determines which item is returned first.
            - *priority* means the priority in the queue
            - Time complexity is O(1) as we are peeking in the head
        """
        loc_value = self._queue.peekitem(0)  # type: ignore[attr-defined]
        return (loc_value[0].priority, loc_value[1])
