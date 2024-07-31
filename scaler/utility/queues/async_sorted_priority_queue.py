from asyncio import Queue
from typing import Any, Dict

from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue


class AsyncSortedPriorityQueue(Queue):
    """A subclass of Queue; retrieves entries in priority order (lowest first), and then by adding order.

    Entries are typically list of the form: [priority number, data].
    """

    def __len__(self):
        return len(self._queue)

    def _init(self, maxsize: int):
        self._queue = AsyncPriorityQueue()

        # Keeps an item count to assign monotonic integer to queued items, so to also keep the priority queue sorted by
        # adding order.
        # See https://docs.python.org/3/library/heapq.html#priority-queue-implementation-notes.
        self._item_counter: int = 0
        self._data_to_item_id: Dict[Any, int] = dict()

    def _put(self, item) -> None:
        priority, data = item

        if data in self._data_to_item_id:
            raise ValueError(f"item `{data}` already in the queue")

        item_id = self._item_counter
        self._item_counter += 1

        self._queue._put([priority, (item_id, data)])
        self._data_to_item_id[data] = item_id

    def _get(self):
        priority, (_, data) = self._queue._get()
        self._data_to_item_id.pop(data)

        return [priority, data]

    def remove(self, data: Any) -> None:
        item_id = self._data_to_item_id.pop(data)
        self._queue.remove((item_id, data))
