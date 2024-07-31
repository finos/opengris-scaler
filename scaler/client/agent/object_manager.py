from typing import Optional

from scaler.client.agent.mixins import ObjectManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import (
    ObjectContent,
    ObjectInstruction,
    ObjectInstructionType,
    ObjectRequest,
    ObjectRequestType,
)


class ClientObjectManager(ObjectManager):
    def __init__(self, identity: bytes):
        self._sent_object_ids = set()
        self._identity = identity

        self._connector_internal: Optional[AsyncConnector] = None
        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_internal: AsyncConnector, connector_external: AsyncConnector):
        self._connector_internal = connector_internal
        self._connector_external = connector_external

    async def on_object_instruction(self, instruction: ObjectInstruction):
        if instruction.type == ObjectInstructionType.Create:
            await self.__send_object_creation(instruction)
        elif instruction.type == ObjectInstructionType.Delete:
            await self.__delete_objects(instruction)

    async def on_object_request(self, object_request: ObjectRequest):
        assert object_request.type == ObjectRequestType.Get
        await self._connector_external.send(object_request)

    def record_task_result(self, task_id: bytes, object_id: bytes):
        self._sent_object_ids.add(object_id)

    async def clean_all_objects(self):
        await self._connector_external.send(
            ObjectInstruction(ObjectInstructionType.Delete, self._identity, ObjectContent(tuple(self._sent_object_ids)))
        )
        self._sent_object_ids = set()

    async def __send_object_creation(self, instruction: ObjectInstruction):
        assert instruction.type == ObjectInstructionType.Create

        new_object_content = list(
            zip(
                *filter(
                    lambda object_pack: object_pack[0] not in self._sent_object_ids,
                    zip(
                        instruction.object_content.object_ids,
                        instruction.object_content.object_names,
                        instruction.object_content.object_bytes,
                    ),
                )
            )
        )

        if not new_object_content:
            return

        instruction.object_content = ObjectContent(*new_object_content)

        self._sent_object_ids.update(instruction.object_content.object_ids)
        await self._connector_external.send(instruction)

    async def __delete_objects(self, instruction: ObjectInstruction):
        assert instruction.type == ObjectInstructionType.Delete
        self._sent_object_ids.difference_update(instruction.object_content.object_ids)
        await self._connector_external.send(instruction)
