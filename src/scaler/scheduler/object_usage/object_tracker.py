import abc
import itertools
from typing import Callable, Dict, Generator, Generic, List, Optional, Set, Tuple, TypeVar

from scaler.utility.many_to_many_dict import ManyToManyDict

BlockType = TypeVar("BlockType")
ObjectKeyType = TypeVar("ObjectKeyType")


class ObjectUsage(Generic[ObjectKeyType], metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_object_key(self) -> ObjectKeyType:
        raise NotImplementedError()

    @abc.abstractmethod
    def get_object_size(self) -> int:
        raise NotImplementedError()


ObjectType = TypeVar("ObjectType", bound=ObjectUsage)


class ObjectTracker(Generic[BlockType, ObjectKeyType, ObjectType]):
    def __init__(self, prefix: str, callback: Callable[[ObjectType], None]):
        self._prefix = prefix
        self._callback = callback

        self._current_blocks: Set[BlockType] = set()
        self._object_key_to_block: ManyToManyDict[ObjectKeyType, BlockType] = ManyToManyDict()
        self._object_key_to_object: Dict[ObjectKeyType, ObjectType] = dict()
        # Objects by size class, the size's bit length, so `largest` reads the big ones without sorting the store.
        self._objects_by_size_class: Dict[int, Dict[ObjectKeyType, ObjectType]] = dict()

    def object_count(self):
        return len(self._object_key_to_object)

    def largest(self, limit: int) -> List[ObjectType]:
        """The `limit` biggest objects, biggest first, taken a size class at a time.

        Every object at least twice the size of the smallest one returned is in the list, and the class the list
        ends in can be partial. The cost is the number of classes plus the limit, never the whole store.
        """
        largest: List[ObjectType] = []
        for size_class in sorted(self._objects_by_size_class, reverse=True):
            if len(largest) >= limit:
                break

            members = self._objects_by_size_class[size_class].values()
            largest.extend(itertools.islice(members, limit - len(largest)))

        largest.sort(key=lambda obj: obj.get_object_size(), reverse=True)
        return largest

    def get_all_object_keys(self) -> Set[ObjectKeyType]:
        return set(self._object_key_to_object.keys())

    def has_object(self, key: ObjectKeyType) -> bool:
        return key in self._object_key_to_object

    def get_object(self, key: ObjectKeyType) -> ObjectType:
        return self._object_key_to_object[key]

    def add_object(self, obj: ObjectType):
        key = obj.get_object_key()
        replaced = self._object_key_to_object.get(key)
        if replaced is not None:
            self.__forget_size_class(replaced)

        self._object_key_to_object[key] = obj
        self._objects_by_size_class.setdefault(obj.get_object_size().bit_length(), dict())[key] = obj

    def get_object_block_pairs(self, blocks: Set[BlockType]) -> Generator[Tuple[ObjectKeyType, BlockType], None, None]:
        for block in blocks:
            if not self._object_key_to_block.has_right_key(block):
                continue

            for object_key in self._object_key_to_block.get_left_items(block):
                yield object_key, block

    def add_blocks_for_one_object(self, object_key: ObjectKeyType, blocks: Set[BlockType]):
        if object_key not in self._object_key_to_object:
            raise KeyError(f"cannot find key={object_key} in ObjectTracker")

        for block in blocks:
            self._object_key_to_block.add(object_key, block)

        self._current_blocks.update(blocks)

    def remove_blocks_for_one_object(self, object_key: ObjectKeyType, blocks: Set[BlockType]):
        ready_objects = []
        for block in blocks:
            obj = self.__remove_block_for_object(object_key, block)
            if obj is None:
                continue

            ready_objects.append(obj)

        for obj in ready_objects:
            self._callback(obj)

    def add_one_block_for_objects(self, object_keys: Set[ObjectKeyType], block: BlockType):
        for object_key in object_keys:
            if object_key not in self._object_key_to_object:
                raise KeyError(f"cannot find key={object_key} in ObjectTracker")

            self._object_key_to_block.add(object_key, block)

        self._current_blocks.add(block)

    def remove_one_block_for_objects(self, object_keys: Set[ObjectKeyType], block: BlockType):
        ready_objects = []
        for object_key in object_keys:
            obj = self.__remove_block_for_object(object_key, block)
            if obj is None:
                continue

            ready_objects.append(obj)

        for obj in ready_objects:
            self._callback(obj)

    def remove_blocks(self, blocks: Set[BlockType]):
        ready_objects = []
        for block in blocks:
            if not self._object_key_to_block.has_right_key(block):
                continue

            object_keys = self._object_key_to_block.get_left_items(block).copy()
            for object_key in object_keys:
                obj = self.__remove_block_for_object(object_key, block)
                if obj is None:
                    continue

                ready_objects.append(obj)

        for obj in ready_objects:
            self._callback(obj)

    def __remove_block_for_object(self, object_key: ObjectKeyType, block: BlockType) -> Optional[ObjectType]:
        if block not in self._current_blocks:
            return None

        if object_key not in self._object_key_to_object:
            return None

        if not self._object_key_to_block.has_key_pair(object_key, block):
            return None

        self._object_key_to_block.remove(object_key, block)

        if not self._object_key_to_block.has_right_key(block):
            self._current_blocks.remove(block)

        if self._object_key_to_block.has_left_key(object_key):
            return None

        obj = self._object_key_to_object.pop(object_key)
        self.__forget_size_class(obj)
        return obj

    def __forget_size_class(self, obj: ObjectType) -> None:
        size_class = obj.get_object_size().bit_length()
        members = self._objects_by_size_class[size_class]
        members.pop(obj.get_object_key())
        if not members:
            self._objects_by_size_class.pop(size_class)
