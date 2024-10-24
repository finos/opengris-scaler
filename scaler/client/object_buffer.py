import dataclasses
import pickle
from typing import Any, Callable, List, Optional, Set

import cloudpickle

from scaler.client.serializer.mixins import Serializer
from scaler.io.sync_connector import SyncConnector
from scaler.io.utility import chunk_to_list_of_bytes
from scaler.protocol.python.common import ObjectContent
from scaler.protocol.python.message import ObjectInstruction
from scaler.utility.object_utility import generate_object_id, generate_serializer_object_id


@dataclasses.dataclass
class ObjectCache:
    object_id: bytes
    object_name: bytes
    object_bytes: List[bytes]


class ObjectBuffer:
    def __init__(self, identity: bytes, serializer: Serializer, connector: SyncConnector):
        self._identity = identity
        self._serializer = serializer
        self._connector = connector

        self._pending_objects: List[ObjectCache] = list()
        self._pending_delete_objects: Set[bytes] = set()

    def buffer_send_serializer(self) -> ObjectCache:
        serializer_cache = self.__construct_serializer()

        self._pending_objects.append(serializer_cache)
        return serializer_cache

    def buffer_send_function(self, fn: Callable) -> ObjectCache:
        function_cache = self.__construct_function(fn)

        self._pending_objects.append(function_cache)
        return function_cache

    def buffer_send_object(self, obj: Any, name: Optional[str] = None) -> ObjectCache:
        object_cache = self.__construct_object(obj, name)

        self._pending_objects.append(object_cache)
        return object_cache

    def buffer_delete_object(self, object_ids: Set[bytes]):
        self._pending_delete_objects.update(object_ids)

    def commit_send_objects(self):
        if not self._pending_objects:
            return

        objects_to_send = [
            (obj_cache.object_id, obj_cache.object_name, obj_cache.object_bytes) for obj_cache in self._pending_objects
        ]

        self._connector.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Create,
                self._identity,
                ObjectContent.new_msg(*zip(*objects_to_send)),
            )
        )

        self._pending_objects = list()

    def commit_delete_objects(self):
        if not self._pending_delete_objects:
            return

        self._connector.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Delete,
                self._identity,
                ObjectContent.new_msg(tuple(self._pending_delete_objects)),
            )
        )

        self._pending_delete_objects.clear()

    def __construct_serializer(self) -> ObjectCache:
        serializer_bytes = cloudpickle.dumps(self._serializer, protocol=pickle.HIGHEST_PROTOCOL)
        object_id = generate_serializer_object_id(self._identity)
        return ObjectCache(object_id, b"serializer", chunk_to_list_of_bytes(serializer_bytes))

    def __construct_function(self, fn: Callable) -> ObjectCache:
        function_bytes = self._serializer.serialize(fn)
        object_id = generate_object_id(self._identity, function_bytes)
        function_cache = ObjectCache(
            object_id,
            getattr(fn, "__name__", f"<func {object_id.hex()[:6]}>").encode(),
            chunk_to_list_of_bytes(function_bytes),
        )
        return function_cache

    def __construct_object(self, obj: Any, name: Optional[str] = None) -> ObjectCache:
        object_payload = self._serializer.serialize(obj)
        object_id = generate_object_id(self._identity, object_payload)
        name_bytes = name.encode() if name else f"<obj {object_id.hex()[-6:]}>".encode()
        return ObjectCache(object_id, name_bytes, chunk_to_list_of_bytes(object_payload))
