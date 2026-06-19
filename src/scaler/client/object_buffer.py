import dataclasses
import hashlib
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

        # Two dedup layers so data handed to many tasks (across separate submit() / map() /
        # get() calls) is serialized and uploaded once:
        #  - identity dedup (keyed by id(obj)) skips re-serializing the same object.
        #    _dedup_alive holds a parallel weakref so a recycled id() can't serve a stale
        #    snapshot; non-weakref-able objects dedup only within a commit cycle (the per-cycle
        #    cache). Objects mutated in place opt out via reserialize=True.
        #  - content dedup (keyed by md5 of the payload) skips re-uploading identical bytes by
        #    reusing the object ID first minted for that payload. IDs are random, not derived
        #    from content, so an ID minted after clear() can't collide with one a concurrent
        #    clear()-driven delete is removing.
        self._dedup_alive: "weakref.WeakValueDictionary[int, Any]" = weakref.WeakValueDictionary()
        self._dedup_cache: Dict[int, ObjectCache] = {}
        self._cycle_dedup_cache: Dict[int, ObjectCache] = {}
        self._payload_hash_to_object_id: Dict[bytes, ObjectID] = {}

        self._serializer_object_id = self.__send_serializer()

    def buffer_send_function(self, fn: Callable) -> ObjectCache:
        cached = self.__lookup_dedup(fn, ignore_persistent=False)
        if cached is not None:
            return cached
        cache = self.__buffer_send_serialized_object(self.__construct_function(fn))
        self.__remember_dedup(fn, cache)
        return cache

    def buffer_send_object(self, obj: Any, name: Optional[str], *, reserialize: bool, dedup: bool) -> ObjectCache:
        # dedup=False skips the identity cache (used by send_object(), which hands the object
        # back to the user); content dedup still applies. reserialize=True ignores a snapshot
        # cached before this call, in case `obj` was mutated in place.
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

        # Drop only the per-cycle cache; the persistent caches survive so a payload reused
        # across separate submit() calls uploads once.
        self._cycle_dedup_cache.clear()

    def clear(self):
        """
        remove all committed and pending objects.
        """

        self._pending_objects.clear()

        # the Clear instruction does not clear the serializer.
        self._valid_object_ids.clear()
        self._valid_object_ids.add(self._serializer_object_id)

        # Drop every dedup cache too: the server discarded these objects. A later upload of the
        # same content then mints a fresh object_id, so it can't collide with one this clear()
        # is about to delete server-side.
        self._dedup_alive = weakref.WeakValueDictionary()
        self._dedup_cache.clear()
        self._cycle_dedup_cache.clear()
        self._payload_hash_to_object_id.clear()

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
        object_id = self.__object_id_for_payload(function_payload)
        function_cache = ObjectCache(
            object_id,
            ObjectMetadata.ObjectContentType.object,
            getattr(fn, "__name__", f"<func {repr(object_id)}>").encode(),
            function_payload,
        )

        return function_cache

    def __construct_object(self, obj: Any, name: Optional[str]) -> ObjectCache:
        object_payload = self._serializer.serialize(obj)
        object_id = self.__object_id_for_payload(object_payload)
        name_bytes = name.encode() if name else f"<obj {repr(object_id)}>".encode()
        object_cache = ObjectCache(object_id, ObjectMetadata.ObjectContentType.object, name_bytes, object_payload)

        return object_cache

    def __object_id_for_payload(self, payload: bytes) -> ObjectID:
        # Reuse the object ID already minted for this exact payload (so the upload is skipped);
        # mint a fresh random ID the first time a payload is seen this session.
        payload_hash = hashlib.md5(payload).digest()
        object_id = self._payload_hash_to_object_id.get(payload_hash)
        if object_id is None or object_id not in self._valid_object_ids:
            object_id = ObjectID.generate_object_id(self._identity)
            self._payload_hash_to_object_id[payload_hash] = object_id
        return object_id

    def __buffer_send_serialized_object(self, object_cache: ObjectCache) -> ObjectCache:
        # Skip the upload if we already sent this object_id this session. _valid_object_ids is
        # assumed to reflect server state, which holds because the client only sends
        # create/clear and its objects live until it clear()s or disconnects (both reset
        # _valid_object_ids). The lone gap -- the scheduler wrongly declaring a live client
        # dead, then the client re-buffering an equal object -- would stall that task, never
        # corrupt data.
        if object_cache.object_id not in self._valid_object_ids:
            self._pending_objects.append(object_cache)
            self._valid_object_ids.add(object_cache.object_id)

        return object_cache

    def __lookup_dedup(self, obj: Any, *, ignore_persistent: bool) -> Optional[ObjectCache]:
        """Return a live cached ObjectCache for ``obj``, or None.

        The persistent cache is consulted first (unless ``ignore_persistent``), guarded by
        ``_dedup_alive`` so a recycled ``id`` misses instead of serving a stale snapshot. The
        per-cycle cache is consulted second, deduping repeats within the current commit cycle.
        """
        key = id(obj)

        if not ignore_persistent:
            cached = self._dedup_cache.get(key)
            if cached is not None:
                if cached.object_id in self._valid_object_ids and self._dedup_alive.get(key) is obj:
                    return cached
                # The server forgot the object, or `id` was recycled to a different object
                # after the original was collected -- drop it.
                self._dedup_cache.pop(key, None)

        cached = self._cycle_dedup_cache.get(key)
        if cached is not None and cached.object_id in self._valid_object_ids:
            return cached

        return None

    def __remember_dedup(self, obj: Any, cache: ObjectCache) -> None:
        # Always remember in the per-cycle cache (dropped on commit). Weakref-able objects are
        # also remembered persistently so they dedup across cycles, guarded by a weakref against
        # id() reuse.
        key = id(obj)
        self._cycle_dedup_cache[key] = cache
        try:
            self._dedup_alive[key] = obj
        except TypeError:
            # Non-weakref-able (int / str / list / dict / tuple / bytes): can't be guarded
            # against id() reuse, so it never persists across cycles.
            return
        self._dedup_cache[key] = cache

    def __send_serializer(self) -> ObjectID:
        serialized_serializer = self.__construct_serializer()
        self.__buffer_send_serialized_object(serialized_serializer)
        self.commit_send_objects()

        return serialized_serializer.object_id
