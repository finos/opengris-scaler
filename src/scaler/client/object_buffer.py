import dataclasses
import pickle
from typing import Any, Callable, Dict, List, Optional, Set

import cloudpickle

from scaler.client.serializer.mixins import Serializer
from scaler.io.mixins import SyncConnector, SyncObjectStorageConnector
from scaler.protocol.capnp import ObjectInstruction, ObjectMetadata
from scaler.utility.identifiers import ClientID, ObjectID


@dataclasses.dataclass
class ObjectCache:
    object_id: ObjectID
    object_type: ObjectMetadata.ObjectContentType
    object_name: bytes
    object_payload: bytes


class ObjectBuffer:
    def __init__(
        self,
        identity: ClientID,
        serializer: Serializer,
        connector_agent: SyncConnector,
        connector_storage: SyncObjectStorageConnector,
    ):
        self._identity = identity
        self._serializer = serializer

        self._connector_agent = connector_agent
        self._connector_storage = connector_storage

        self._valid_object_ids: Set[ObjectID] = set()
        self._pending_objects: List[ObjectCache] = list()

        # Dedup an object that one operation (submit / map / graph) hands to
        # many tasks: serialize and upload it once, not once per task. Keyed by
        # id(obj) and cleared on every commit, so an entry never outlives its
        # buffering burst and can't serve a stale snapshot if the object is
        # later mutated.
        self._dedup_cache: Dict[int, ObjectCache] = {}

        self._serializer_object_id = self.__send_serializer()

    def buffer_send_function(self, fn: Callable) -> ObjectCache:
        cached = self._dedup_cache.get(id(fn))
        if cached is not None:
            return cached
        cache = self.__buffer_send_serialized_object(self.__construct_function(fn))
        self._dedup_cache[id(fn)] = cache
        return cache

    def buffer_send_object(self, obj: Any, name: Optional[str] = None, dedup: bool = True) -> ObjectCache:
        # dedup=False skips the cache entirely; used by send_object() (see its
        # call site), which buffers without committing.
        if dedup:
            cached = self._dedup_cache.get(id(obj))
            if cached is not None:
                return cached
        cache = self.__buffer_send_serialized_object(self.__construct_object(obj, name))
        if dedup:
            self._dedup_cache[id(obj)] = cache
        return cache

    def commit_send_objects(self):
        if not self._pending_objects:
            return

        object_instructions_to_send = [
            (obj_cache.object_id, obj_cache.object_type, obj_cache.object_name) for obj_cache in self._pending_objects
        ]

        self._connector_agent.send(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.create,
                objectUser=self._identity,
                objectMetadata=ObjectMetadata(
                    objectIds=[object_id for object_id, _, _ in object_instructions_to_send],
                    objectTypes=[object_type for _, object_type, _ in object_instructions_to_send],
                    objectNames=[object_name for _, _, object_name in object_instructions_to_send],
                ),
            )
        )

        for obj_cache in self._pending_objects:
            self._connector_storage.set_object(obj_cache.object_id, obj_cache.object_payload)

        self._pending_objects.clear()

        # Drop the dedup cache so a later cycle can't reuse a pre-mutation snapshot.
        self._dedup_cache.clear()

    def clear(self):
        """
        remove all committed and pending objects.
        """

        self._pending_objects.clear()

        # the Clear instruction does not clear the serializer.
        self._valid_object_ids.clear()
        self._valid_object_ids.add(self._serializer_object_id)

        # Drop the dedup cache too: the server discarded these objects, so a
        # cached entry would name an object_id it no longer knows about.
        self._dedup_cache.clear()

        self._connector_agent.send(
            ObjectInstruction(
                instructionType=ObjectInstruction.ObjectInstructionType.clear,
                objectUser=self._identity,
                objectMetadata=ObjectMetadata(objectIds=()),
            )
        )

    def is_valid_object_id(self, object_id: ObjectID) -> bool:
        return object_id in self._valid_object_ids

    def __construct_serializer(self) -> ObjectCache:
        serializer_payload = cloudpickle.dumps(self._serializer, protocol=pickle.HIGHEST_PROTOCOL)
        object_id = ObjectID.generate_serializer_object_id(self._identity)
        serializer_cache = ObjectCache(
            object_id, ObjectMetadata.ObjectContentType.serializer, b"serializer", serializer_payload
        )

        return serializer_cache

    def __construct_function(self, fn: Callable) -> ObjectCache:
        function_payload = self._serializer.serialize(fn)
        object_id = ObjectID.generate_object_id(self._identity)
        function_cache = ObjectCache(
            object_id,
            ObjectMetadata.ObjectContentType.object,
            getattr(fn, "__name__", f"<func {repr(object_id)}>").encode(),
            function_payload,
        )

        return function_cache

    def __construct_object(self, obj: Any, name: Optional[str] = None) -> ObjectCache:
        object_payload = self._serializer.serialize(obj)
        object_id = ObjectID.generate_object_id(self._identity)
        name_bytes = name.encode() if name else f"<obj {repr(object_id)}>".encode()
        object_cache = ObjectCache(object_id, ObjectMetadata.ObjectContentType.object, name_bytes, object_payload)

        return object_cache

    def __buffer_send_serialized_object(self, object_cache: ObjectCache) -> ObjectCache:
        if object_cache.object_id not in self._valid_object_ids:
            self._pending_objects.append(object_cache)
            self._valid_object_ids.add(object_cache.object_id)

        return object_cache

    def __lookup_dedup(self, obj: Any) -> Optional[ObjectCache]:
        """Return a previously cached ObjectCache for ``obj`` if it is still valid.

        We key on ``id(obj)`` and confirm via a weakref that the same object
        is still alive -- ``id`` values are recycled once an object is GC'd, so
        the weakref check prevents a stale entry from being mis-served.
        """
        key = id(obj)
        cached = self._dedup_cache.get(key)
        if cached is None:
            return None
        # Defensively drop stale entries if the server has forgotten the object
        # (e.g. after ``clear()`` -- though clear() also wipes the cache, this
        # protects against future code paths that invalidate ``_valid_object_ids``
        # without going through clear()).
        if cached.object_id not in self._valid_object_ids:
            self._dedup_cache.pop(key, None)
            return None
        alive = self._dedup_alive.get(key)
        if alive is not obj:
            # Either the original object was GC'd and ``key`` was recycled, or
            # ``obj`` is not weakreffable and was never registered.  Either way
            # we cannot safely reuse the cache entry.
            self._dedup_cache.pop(key, None)
            return None
        return cached

    def __remember_dedup(self, obj: Any, cache: ObjectCache) -> None:
        try:
            self._dedup_alive[id(obj)] = obj
        except TypeError:
            # Non-weakreffable (built-in int/str/tuple/list/dict/...): skip
            # dedup.  These are typically small and cheap to re-serialize.
            return
        self._dedup_cache[id(obj)] = cache

    def __send_serializer(self) -> ObjectID:
        serialized_serializer = self.__construct_serializer()
        self.__buffer_send_serialized_object(serialized_serializer)
        self.commit_send_objects()

        return serialized_serializer.object_id
