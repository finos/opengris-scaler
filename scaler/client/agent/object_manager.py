from typing import Optional, Set

from scaler.client.agent.mixins import ObjectManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import ObjectContent
from scaler.protocol.python.message import ObjectInstruction, ObjectRequest


class ClientObjectManager(ObjectManager):
    def __init__(self, identity: bytes):
        self._sent_object_ids: Set[bytes] = set()
        self._identity = identity

        self._connector_internal: Optional[AsyncConnector] = None
        self._connector_external: Optional[AsyncConnector] = None

    def register(self, connector_internal: AsyncConnector, connector_external: AsyncConnector):
        self._connector_internal = connector_internal
        self._connector_external = connector_external

    async def on_object_instruction(self, instruction: ObjectInstruction):
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create:
            await self.__send_object_creation(instruction)
        elif instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            await self.__delete_objects(instruction)

    async def on_object_request(self, object_request: ObjectRequest):
        assert object_request.request_type == ObjectRequest.ObjectRequestType.Get
        await self._connector_external.send(object_request)

    def record_task_result(self, task_id: bytes, object_id: bytes):
        self._sent_object_ids.add(object_id)

    async def clean_all_objects(self):
        await self._connector_external.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Delete,
                self._identity,
                ObjectContent.new_msg(tuple(self._sent_object_ids)),
            )
        )
        self._sent_object_ids = set()

    async def __send_object_creation(self, instruction: ObjectInstruction):
        assert instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create

        new_object_ids = set(instruction.object_content.object_ids) - self._sent_object_ids
        if not new_object_ids:
            return

        new_object_content = ObjectContent.new_msg(
            *zip(
                *filter(
                    lambda object_pack: object_pack[0] in new_object_ids,
                    zip(
                        instruction.object_content.object_ids,
                        instruction.object_content.object_names,
                        instruction.object_content.object_bytes,
                    ),
                )
            )
        )

        self._sent_object_ids.update(set(new_object_content.object_ids))

        await self._connector_external.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Create, instruction.object_user, new_object_content
            )
        )

    async def __delete_objects(self, instruction: ObjectInstruction):
        assert instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete
        self._sent_object_ids.difference_update(instruction.object_content.object_ids)
        await self._connector_external.send(instruction)
