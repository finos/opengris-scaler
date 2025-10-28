from typing import Dict, Generic, Iterable, Set, Tuple, TypeVar

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")

LeftKeyT = TypeVar("LeftKeyT")
RightKeyT = TypeVar("RightKeyT")


class ManyToManyDict(Generic[LeftKeyT, RightKeyT]):
    def __init__(self):
        self._left_key_to_right_key_set: _KeyValueDictSet[LeftKeyT, RightKeyT] = _KeyValueDictSet()
        self._right_key_to_left_key_set: _KeyValueDictSet[RightKeyT, LeftKeyT] = _KeyValueDictSet()

    def left_keys(self):
        return self._left_key_to_right_key_set.keys()

    def right_keys(self):
        return self._right_key_to_left_key_set.keys()

    def add(self, left_key: LeftKeyT, right_key: RightKeyT):
        self._left_key_to_right_key_set.add(left_key, right_key)
        self._right_key_to_left_key_set.add(right_key, left_key)

    def remove(self, left_key: LeftKeyT, right_key: RightKeyT):
        self._left_key_to_right_key_set.remove_value(left_key, right_key)
        self._right_key_to_left_key_set.remove_value(right_key, left_key)

    def has_left_key(self, left_key: LeftKeyT) -> bool:
        return left_key in self._left_key_to_right_key_set

    def has_right_key(self, right_key: RightKeyT) -> bool:
        return right_key in self._right_key_to_left_key_set

    def has_key_pair(self, left_key: LeftKeyT, right_key: RightKeyT) -> bool:
        return (
            self.has_left_key(left_key)
            and self.has_right_key(right_key)
            and right_key in self._left_key_to_right_key_set.get_values(left_key)
            and left_key in self._right_key_to_left_key_set.get_values(right_key)
        )

    def left_key_items(self) -> Iterable[Tuple[LeftKeyT, Set[RightKeyT]]]:
        return self._left_key_to_right_key_set.items()

    def right_key_items(self) -> Iterable[Tuple[RightKeyT, Set[LeftKeyT]]]:
        return self._right_key_to_left_key_set.items()

    def get_left_items(self, right_key: RightKeyT) -> Set[LeftKeyT]:
        if right_key not in self._right_key_to_left_key_set:
            raise ValueError(f"cannot find {right_key=} in ManyToManyDict")

        return self._right_key_to_left_key_set.get_values(right_key)

    def get_right_items(self, left_key: LeftKeyT) -> Set[RightKeyT]:
        if left_key not in self._left_key_to_right_key_set:
            raise ValueError(f"cannot find {left_key=} in ManyToManyDict")

        return self._left_key_to_right_key_set.get_values(left_key)

    def remove_left_key(self, left_key: LeftKeyT) -> Set[RightKeyT]:
        if left_key not in self._left_key_to_right_key_set:
            raise KeyError(f"cannot find {left_key=} in ManyToManyDict")

        right_keys = self._left_key_to_right_key_set.remove_key(left_key)
        for right_key in right_keys:
            self._right_key_to_left_key_set.remove_value(right_key, left_key)

        return right_keys

    def remove_right_key(self, right_key: RightKeyT) -> Set[LeftKeyT]:
        if right_key not in self._right_key_to_left_key_set:
            raise ValueError(f"cannot find {right_key=} in ManyToManyDict")

        left_keys = self._right_key_to_left_key_set.remove_key(right_key)
        for left_key in left_keys:
            self._left_key_to_right_key_set.remove_value(left_key, right_key)

        return left_keys


class _KeyValueDictSet(Generic[KeyT, ValueT]):
    def __init__(self):
        self._key_to_value_set: Dict[KeyT, Set[ValueT]] = dict()

    def __contains__(self, key) -> bool:
        return key in self._key_to_value_set

    def keys(self):
        return self._key_to_value_set.keys()

    def values(self):
        return self._key_to_value_set.values()

    def items(self):
        return self._key_to_value_set.items()

    def add(self, key: KeyT, value: ValueT):
        if key not in self._key_to_value_set:
            self._key_to_value_set[key] = set()

        self._key_to_value_set[key].add(value)

    def get_values(self, key: KeyT) -> Set[ValueT]:
        if key not in self._key_to_value_set:
            raise ValueError(f"cannot find {key=} in KeyValueSet")

        return self._key_to_value_set[key]

    def remove_key(self, key: KeyT) -> Set[ValueT]:
        if key not in self._key_to_value_set:
            raise KeyError(f"cannot find {key=} in KeyValueSet")

        values = self._key_to_value_set.pop(key)
        return values

    def remove_value(self, key: KeyT, value: ValueT):
        if key not in self._key_to_value_set:
            raise KeyError(f"cannot find {key=} in KeyValueSet")

        self._key_to_value_set[key].remove(value)
        if not self._key_to_value_set[key]:
            self._key_to_value_set.pop(key)
