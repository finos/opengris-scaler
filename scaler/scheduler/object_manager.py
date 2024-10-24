import dataclasses
import logging
from asyncio import Queue
from typing import List, Optional, Set

from scaler.io.async_binder import AsyncBinder
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import ObjectContent
from scaler.protocol.python.message import ObjectInstruction, ObjectRequest, ObjectResponse
from scaler.protocol.python.status import ObjectManagerStatus
from scaler.scheduler.mixins import ClientManager, ObjectManager, WorkerManager
from scaler.scheduler.object_usage.object_tracker import ObjectTracker, ObjectUsage
from scaler.utility.formatter import format_bytes
from scaler.utility.mixins import Looper, Reporter


@dataclasses.dataclass
class _ObjectCreation(ObjectUsage):
    object_id: bytes
    object_creator: bytes
    object_name: bytes
    object_bytes: List[bytes]

    def get_object_key(self) -> bytes:
        return self.object_id


class VanillaObjectManager(ObjectManager, Looper, Reporter):
    def __init__(self):
        self._object_storage: ObjectTracker[bytes, _ObjectCreation] = ObjectTracker(
            "object_usage", self.__finished_object_storage
        )

        self._queue_deleted_object_ids: Queue[bytes] = Queue()

        self._binder: Optional[AsyncBinder] = None
        self._binder_monitor: Optional[AsyncConnector] = None
        self._client_manager: Optional[ClientManager] = None
        self._worker_manager: Optional[WorkerManager] = None

    def register(
        self,
        binder: AsyncBinder,
        binder_monitor: AsyncConnector,
        client_manager: ClientManager,
        worker_manager: WorkerManager,
    ):
        self._binder = binder
        self._binder_monitor = binder_monitor
        self._client_manager = client_manager
        self._worker_manager = worker_manager

    async def on_object_instruction(self, source: bytes, instruction: ObjectInstruction):
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create:
            self.__on_object_create(source, instruction)
            return

        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            self.on_del_objects(instruction.object_user, set(instruction.object_content.object_ids))
            return

        logging.error(
            f"received unknown object response type instruction_type={instruction.instruction_type} from "
            f"source={instruction.object_user}"
        )

    async def on_object_request(self, source: bytes, request: ObjectRequest):
        if request.request_type == ObjectRequest.ObjectRequestType.Get:
            await self.__process_get_request(source, request)
            return

        logging.error(f"received unknown object request type {request=} from {source=!r}")

    def on_add_object(self, object_user: bytes, object_id: bytes, object_name: bytes, object_bytes: List[bytes]):
        creation = _ObjectCreation(object_id, object_user, object_name, object_bytes)
        logging.debug(
            f"add object cache "
            f"object_name={creation.object_name!r}, "
            f"object_id={creation.object_id.hex()}, "
            f"size={format_bytes(len(creation.object_bytes))}"
        )

        self._object_storage.add_object(creation)
        self._object_storage.add_blocks_for_one_object(creation.get_object_key(), {creation.object_creator})

    def on_del_objects(self, object_user: bytes, object_ids: Set[bytes]):
        for object_id in object_ids:
            self._object_storage.remove_one_block_for_objects({object_id}, object_user)

    def clean_client(self, client: bytes):
        self._object_storage.remove_blocks({client})

    async def routine(self):
        await self.__routine_send_objects_deletions()

    def has_object(self, object_id: bytes) -> bool:
        return self._object_storage.has_object(object_id)

    def get_object_name(self, object_id: bytes) -> bytes:
        if not self.has_object(object_id):
            return b"<Unknown>"

        return self._object_storage.get_object(object_id).object_name

    def get_object_content(self, object_id: bytes) -> List[bytes]:
        if not self.has_object(object_id):
            return list()

        return self._object_storage.get_object(object_id).object_bytes

    def get_status(self) -> ObjectManagerStatus:
        return ObjectManagerStatus.new_msg(
            self._object_storage.object_count(),
            sum(sum(map(len, v.object_bytes)) for _, v in self._object_storage.items()),
        )

    async def __process_get_request(self, source: bytes, request: ObjectRequest):
        await self._binder.send(source, self.__construct_response(request))

    async def __routine_send_objects_deletions(self):
        deleted_object_ids = [await self._queue_deleted_object_ids.get()]
        self._queue_deleted_object_ids.task_done()

        while not self._queue_deleted_object_ids.empty():
            deleted_object_ids.append(self._queue_deleted_object_ids.get_nowait())
            self._queue_deleted_object_ids.task_done()

        for worker in self._worker_manager.get_worker_ids():
            await self._binder.send(
                worker,
                ObjectInstruction.new_msg(
                    ObjectInstruction.ObjectInstructionType.Delete,
                    worker,
                    ObjectContent.new_msg(tuple(deleted_object_ids)),
                ),
            )

    def __on_object_create(self, source: bytes, instruction: ObjectInstruction):
        if not self._client_manager.has_client_id(instruction.object_user):
            logging.error(f"received object creation from {source!r} for unknown client {instruction.object_user!r}")
            return

        for object_id, object_name, object_bytes in zip(
            instruction.object_content.object_ids,
            instruction.object_content.object_names,
            instruction.object_content.object_bytes,
        ):
            self.on_add_object(instruction.object_user, object_id, object_name, object_bytes)

    def __finished_object_storage(self, creation: _ObjectCreation):
        logging.debug(
            f"del object cache "
            f"object_name={creation.object_name!r}, "
            f"object_id={creation.object_id.hex()}, "
            f"size={format_bytes(len(creation.object_bytes))}"
        )
        self._queue_deleted_object_ids.put_nowait(creation.object_id)

    def __construct_response(self, request: ObjectRequest) -> ObjectResponse:
        object_ids = []
        object_names = []
        object_bytes = []
        for object_id in request.object_ids:
            if not self.has_object(object_id):
                continue

            object_info = self._object_storage.get_object(object_id)
            object_ids.append(object_info.object_id)
            object_names.append(object_info.object_name)
            object_bytes.append(object_info.object_bytes)

        return ObjectResponse.new_msg(
            ObjectResponse.ObjectResponseType.Content,
            ObjectContent.new_msg(tuple(request.object_ids), tuple(object_names), tuple(object_bytes)),
        )
