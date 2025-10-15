import collections
from typing import Callable


class EventList(collections.UserList):
    """A list that emits events when it is modified."""

    def __init__(self, initlist=None):
        super().__init__(initlist=initlist)
        self._callbacks = []

    def add_update_callback(self, callback: Callable[["EventList"], None]):
        self._callbacks.append(callback)

    def __setitem__(self, i, item):
        super().__setitem__(i, item)
        self._list_updated()

    def __delitem__(self, i):
        super().__delitem__(i)
        self._list_updated()

    def __add__(self, other):
        super().__add__(other)
        self._list_updated()

    def __iadd__(self, other):
        super().__iadd__(other)
        self._list_updated()
        return self

    def append(self, item):
        super().append(item)
        self._list_updated()

    def insert(self, i, item):
        super().insert(i, item)
        self._list_updated()

    def pop(self, i: int = -1):
        v = super().pop(i)
        self._list_updated()
        return v

    def remove(self, item):
        super().remove(item)
        self._list_updated()

    def clear(self) -> None:
        super().clear()
        self._list_updated()

    def sort(self, /, *args, **kwargs):
        super().sort(*args, **kwargs)
        self._list_updated()

    def extend(self, other) -> None:
        super().extend(other)
        self._list_updated()

    def _list_updated(self):
        for callback in self._callbacks:
            callback(self)
