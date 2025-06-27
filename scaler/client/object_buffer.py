import dataclasses
import pickle
from typing import Any, Callable, List, Optional, Set

import cloudpickle

from scaler.client.serializer.mixins import Serializer
from scaler.io.sync_connector import SyncConnector
from scaler.io.sync_object_storage_connector import SyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata
from scaler.protocol.python.message import ObjectInstruction
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

        self._serializer_object_id = self.__send_serializer()

    def buffer_send_function(self, fn: Callable) -> ObjectCache:
        return self.__buffer_send_serialized_object(self.__construct_function(fn))

    def buffer_send_object(self, obj: Any, name: Optional[str] = None) -> ObjectCache:
        return self.__buffer_send_serialized_object(self.__construct_object(obj, name))

    def commit_send_objects(self):
        if not self._pending_objects:
            return

        object_instructions_to_send = [
            (obj_cache.object_id, obj_cache.object_type, obj_cache.object_name)
            for obj_cache in self._pending_objects
        ]

        self._connector_agent.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Create,
                self._identity,
                ObjectMetadata.new_msg(*zip(*object_instructions_to_send)),
            )
        )

        for obj_cache in self._pending_objects:
            self._connector_storage.set_object(obj_cache.object_id, obj_cache.object_payload)

        self._pending_objects.clear()

    def clear(self):
        """
        remove all committed and pending objects.
        """

        self._pending_objects.clear()

        # the Clear instruction does not clear the serializer.
        self._valid_object_ids.clear()
        self._valid_object_ids.add(self._serializer_object_id)

        self._connector_agent.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Clear, self._identity, ObjectMetadata.new_msg(tuple())
            )
        )

    def is_valid_object_id(self, object_id: ObjectID) -> bool:
        return object_id in self._valid_object_ids

    def __construct_serializer(self) -> ObjectCache:
        serializer_payload = cloudpickle.dumps(self._serializer, protocol=pickle.HIGHEST_PROTOCOL)
        object_id = ObjectID.generate_serializer_object_id(self._identity)
        serializer_cache = ObjectCache(
            object_id,
            ObjectMetadata.ObjectContentType.Serializer,
            b"serializer",
            serializer_payload,
        )

        return serializer_cache

    def __construct_function(self, fn: Callable) -> ObjectCache:
        function_payload = self._serializer.serialize(fn)
        object_id = ObjectID.generate_object_id(self._identity)
        function_cache = ObjectCache(
            object_id,
            ObjectMetadata.ObjectContentType.Object,
            getattr(fn, "__name__", f"<func {repr(object_id)}>").encode(),
            function_payload,
        )

        return function_cache

    def __construct_object(self, obj: Any, name: Optional[str] = None) -> ObjectCache:
        object_payload = self._serializer.serialize(obj)
        object_id = ObjectID.generate_object_id(self._identity)
        name_bytes = name.encode() if name else f"<obj {repr(object_id)}>".encode()
        object_cache = ObjectCache(object_id, ObjectMetadata.ObjectContentType.Object, name_bytes, object_payload)

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
