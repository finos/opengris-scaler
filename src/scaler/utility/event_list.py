import collections
import sys
from typing import Any, Callable, Iterable, Optional, SupportsIndex, TypeVar, Union

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

ItemType = TypeVar("ItemType")


class EventList(collections.UserList[ItemType]):
    """A list that emits events when it is modified."""

    def __init__(self, initlist: Optional[Iterable[ItemType]] = None):
        super().__init__(initlist=initlist)
        self._callbacks = []

    def add_update_callback(self, callback: Callable[["EventList[ItemType]"], None]):
        self._callbacks.append(callback)

    @override
    def __setitem__(self, i: Union[SupportsIndex, slice], item: Any):
        super().__setitem__(i, item)
        self._list_updated()

    @override
    def __delitem__(self, i: Union[SupportsIndex, slice]):
        super().__delitem__(i)
        self._list_updated()

    @override
    def __iadd__(self, other: Iterable[ItemType]):
        super().__iadd__(other)
        self._list_updated()
        return self

    @override
    def append(self, item: ItemType):
        super().append(item)
        self._list_updated()

    @override
    def insert(self, i: SupportsIndex, item: ItemType):
        super().insert(i, item)
        self._list_updated()

    @override
    def pop(self, i: SupportsIndex = -1):
        v = super().pop(i)
        self._list_updated()
        return v

    @override
    def remove(self, item: ItemType):
        super().remove(item)
        self._list_updated()

    @override
    def clear(self) -> None:
        super().clear()
        self._list_updated()

    @override
    def sort(self, /, *args: Any, **kwargs: Any):
        super().sort(*args, **kwargs)
        self._list_updated()

    @override
    def extend(self, other: Iterable[ItemType]) -> None:
        super().extend(other)
        self._list_updated()

    def _list_updated(self):
        for callback in self._callbacks:
            callback(self)
