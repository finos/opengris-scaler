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

        # Identity-based dedup cache, scoped to a single commit cycle (one
        # submit() / map() / graph() worth of buffering).  Avoids re-serializing
        # and re-uploading the same Python object that one operation passes to
        # many tasks (e.g. a shared DataFrame / numpy array in map / parfun /
        # graph).  Keyed by ``id(obj)``.  ``commit_send_objects`` and ``clear``
        # drop it, so an entry never spans a point where user code could regain
        # control and mutate the object -- the cache only ever serves a snapshot
        # identical to what re-serializing right now would produce, which keeps
        # this strictly equivalent-or-better than re-serializing every call.
        # Every object buffered within a cycle is kept alive by its caller, so
        # ``id`` cannot be recycled while an entry lives; a plain dict (no
        # weakref) is therefore both sufficient and safe, and it lets us dedup
        # non-weakreffable args (list / dict / tuple / ...) too.
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
        # ``dedup`` is opt-out for the standalone send_object() path: that call
        # buffers without committing and then hands control back to the user, so
        # leaving an entry behind could serve a stale snapshot to a later submit
        # if the object is mutated in between.  Task buffering (submit / map /
        # graph) happens in one uninterrupted burst, where dedup is always safe.
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

        # Scope dedup to a single commit cycle: once these objects are uploaded
        # the user regains control and may mutate them, so the next cycle must
        # re-serialize rather than risk serving a stale snapshot.
        self._dedup_cache.clear()

    def clear(self):
        """
        remove all committed and pending objects.
        """

        self._pending_objects.clear()

        # the Clear instruction does not clear the serializer.
        self._valid_object_ids.clear()
        self._valid_object_ids.add(self._serializer_object_id)

        # Drop the dedup cache too: the server side has discarded the objects,
        # so reusing a previously-cached ObjectCache would refer to an
        # object_id the server no longer knows about.
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

    def __send_serializer(self) -> ObjectID:
        serialized_serializer = self.__construct_serializer()
        self.__buffer_send_serialized_object(serialized_serializer)
        self.commit_send_objects()

        return serialized_serializer.object_id
