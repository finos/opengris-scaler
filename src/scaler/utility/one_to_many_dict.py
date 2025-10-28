from typing import Dict, Generic, Set, TypeVar

KeyT = TypeVar("KeyT")
ValueT = TypeVar("ValueT")


class OneToManyDict(Generic[KeyT, ValueT]):
    def __init__(self):
        self._key_to_value_set: Dict[KeyT, Set[ValueT]] = dict()
        self._value_to_key: Dict[ValueT, KeyT] = dict()

    def __contains__(self, key) -> bool:
        return self.has_key(key)

    def keys(self):
        return self._key_to_value_set.keys()

    def values(self):
        return self._key_to_value_set.values()

    def items(self):
        return self._key_to_value_set.items()

    def add(self, key: KeyT, value: ValueT):
        if value in self._value_to_key and self._value_to_key[value] != key:
            raise ValueError("value has to be unique in OneToManyDict")

        self._value_to_key[value] = key

        if key not in self._key_to_value_set:
            self._key_to_value_set[key] = set()

        self._key_to_value_set[key].add(value)

    def has_key(self, key: KeyT) -> bool:
        return key in self._key_to_value_set

    def has_value(self, value: ValueT) -> bool:
        return value in self._value_to_key

    def get_key(self, value: ValueT) -> KeyT:
        if value not in self._value_to_key:
            raise ValueError(f"cannot find {value=} in OneToManyDict")

        return self._value_to_key[value]

    def get_values(self, key: KeyT) -> Set[ValueT]:
        if key not in self._key_to_value_set:
            raise ValueError(f"cannot find {key=} in OneToManyDict")

        return self._key_to_value_set[key]

    def remove_key(self, key: KeyT) -> Set[ValueT]:
        if key not in self._key_to_value_set:
            raise KeyError(f"cannot find {key=} in OneToManyDict")

        values = self._key_to_value_set.pop(key)
        for value in values:
            self._value_to_key.pop(value)

        return values

    def remove_value(self, value: ValueT) -> KeyT:
        if value not in self._value_to_key:
            raise ValueError(f"cannot find {value=} in OneToManyDict")

        key = self._value_to_key.pop(value)
        self._key_to_value_set[key].remove(value)
        if not self._key_to_value_set[key]:
            self._key_to_value_set.pop(key)

        return key
