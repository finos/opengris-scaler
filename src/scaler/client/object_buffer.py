import dataclasses
import pickle
import weakref
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

        # Identity-based dedup so the same Python object handed to many tasks --
        # across separate submit() / map() / get() calls, not just one batch --
        # is serialized and uploaded once. Keyed by id(obj).
        #
        # _dedup_cache persists across commit cycles; _dedup_alive holds a
        # parallel weakref so an id() recycled after its object was garbage
        # collected can't serve a different object's snapshot. Because the entry
        # survives a commit, an object mutated in place would otherwise serve a
        # stale snapshot -- callers opt out per call via reserialize=True.
        #
        # Non-weakref-able objects (int / str / list / dict / tuple / bytes)
        # can't be guarded against id() reuse, so they dedup only within the
        # current commit cycle (the caller keeps them alive until then) via
        # _cycle_dedup_cache, which commit_send_objects() drops.
        self._dedup_alive: "weakref.WeakValueDictionary[int, Any]" = weakref.WeakValueDictionary()
        self._dedup_cache: Dict[int, ObjectCache] = {}
        self._cycle_dedup_cache: Dict[int, ObjectCache] = {}

        self._serializer_object_id = self.__send_serializer()

    def buffer_send_function(self, fn: Callable) -> ObjectCache:
        cached = self.__lookup_dedup(fn)
        if cached is not None:
            return cached
        cache = self.__buffer_send_serialized_object(self.__construct_function(fn))
        self.__remember_dedup(fn, cache)
        return cache

    def buffer_send_object(
        self, obj: Any, name: Optional[str] = None, *, reserialize: bool = False, dedup: bool = True
    ) -> ObjectCache:
        # dedup=False skips the cache entirely (both lookup and store); used by
        # send_object(), which buffers without committing and hands the object
        # back to the user, who manages reuse explicitly via its ObjectReference.
        #
        # reserialize=True ignores any snapshot cached before this call (it may
        # be stale because `obj` was mutated in place), so the new contents are
        # re-uploaded and the persistent cache is refreshed. Repeats of `obj`
        # within this same commit cycle still dedup against the fresh snapshot.
        if dedup:
            cached = self.__lookup_dedup(obj, ignore_persistent=reserialize)
            if cached is not None:
                return cached
        cache = self.__buffer_send_serialized_object(self.__construct_object(obj, name))
        if dedup:
            self.__remember_dedup(obj, cache)
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

        # Drop only the per-cycle cache; the persistent identity cache survives
        # so a (weakref-able) object reused across separate submit() calls
        # uploads once. (Use reserialize=True to refresh an entry whose object
        # was mutated in place.)
        self._cycle_dedup_cache.clear()

    def clear(self):
        """
        remove all committed and pending objects.
        """

        self._pending_objects.clear()

        # the Clear instruction does not clear the serializer.
        self._valid_object_ids.clear()
        self._valid_object_ids.add(self._serializer_object_id)

        # Drop every dedup cache too: the server discarded these objects, so a
        # cached entry would name an object_id it no longer knows about.
        self._dedup_alive = weakref.WeakValueDictionary()
        self._dedup_cache.clear()
        self._cycle_dedup_cache.clear()

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

    def __lookup_dedup(self, obj: Any, *, ignore_persistent: bool = False) -> Optional[ObjectCache]:
        """Return a live cached ObjectCache for ``obj``, or None.

        The persistent cache is consulted first (unless ``ignore_persistent``,
        set for reserialize), guarded by ``_dedup_alive`` so a recycled ``id``
        (original object collected, ``id`` reused by another object) misses
        instead of serving a stale snapshot. The per-cycle cache is consulted
        second; it dedups repeats within the current commit cycle, including the
        fresh snapshot produced by a reserialize.
        """
        key = id(obj)

        if not ignore_persistent:
            cached = self._dedup_cache.get(key)
            if cached is not None:
                if cached.object_id in self._valid_object_ids and self._dedup_alive.get(key) is obj:
                    return cached
                # The server forgot the object, or `id` was recycled to a
                # different object after the original was collected -- drop it.
                self._dedup_cache.pop(key, None)

        cached = self._cycle_dedup_cache.get(key)
        if cached is not None and cached.object_id in self._valid_object_ids:
            return cached

        return None

    def __remember_dedup(self, obj: Any, cache: ObjectCache) -> None:
        # The per-cycle cache always remembers the object so repeats within this
        # commit cycle dedup; commit_send_objects() then drops it. Weakref-able
        # objects are additionally remembered in the persistent cache so they
        # dedup across cycles, guarded by a parallel weakref against id() reuse.
        key = id(obj)
        self._cycle_dedup_cache[key] = cache
        try:
            self._dedup_alive[key] = obj
        except TypeError:
            # Non-weakref-able (int / str / list / dict / tuple / bytes): can't
            # be guarded against id() reuse, so it never persists across cycles.
            return
        self._dedup_cache[key] = cache

    def __send_serializer(self) -> ObjectID:
        serialized_serializer = self.__construct_serializer()
        self.__buffer_send_serialized_object(serialized_serializer)
        self.commit_send_objects()

        return serialized_serializer.object_id
