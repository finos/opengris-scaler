import sys
from asyncio import Queue
from typing import Any, Sequence, Tuple, Union

from scaler.utility.queues.stable_priority_queue import StablePriorityQueue

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

PriorityType = Union[int, Tuple["PriorityType", ...]]


class AsyncPriorityQueue(Queue[Sequence[Any]]):
    """A subclass of Queue; retrieves entries in priority order (lowest first).

    Input entries are typically list of the form: [priority, data].
    """

    def __len__(self):
        return len(self._queue)

    @override
    def _init(self, maxsize: int):
        self._queue: StablePriorityQueue[PriorityType, Any] = StablePriorityQueue()

    @override
    def _put(self, item: Sequence[Any]):
        if not isinstance(item, list):
            item = list(item)

        priority, data = item
        self._queue.put(priority, data)

    @override
    def _get(self):
        return self._queue.get()

    def remove(self, data: Any):
        self._queue.remove(data)

    def decrease_priority(self, data: Any):
        # Decrease the priority *value* of an item in the queue, effectively move data closer to the front.
        # Notes:
        #     - *priority* in the signature means the priority *value* of the item.
        #     - Time complexity is O(log n) due to the underlying SortedDict structure.
        self._queue.decrease_priority(data)

    def max_priority_item(self) -> Tuple[PriorityType, Any]:
        """Return the current item at the front of the queue without removing it from the queue.

        Notes:
            - This is a "peek" operation; it does not modify the queue.
            - For items with the same priority, insertion order determines which item is returned first.
            - *priority* means the priority in the queue
            - Time complexity is O(1) as we are peeking in the head
        """
        return self._queue.max_priority_item()
