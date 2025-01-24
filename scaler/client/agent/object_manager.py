from typing import Optional, Set

from scaler.client.agent.mixins import ObjectManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import ObjectContent
from scaler.protocol.python.message import ObjectInstruction, ObjectRequest, TaskResult


class ClientObjectManager(ObjectManager):
    def __init__(self, identity: bytes):
        self._sent_object_ids: Set[bytes] = set()
        self._sent_serializer_id: Optional[bytes] = None

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
        elif instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Clear:
            await self.clear_all_objects(clear_serializer=False)

    async def on_object_request(self, object_request: ObjectRequest):
        assert object_request.request_type == ObjectRequest.ObjectRequestType.Get
        await self._connector_external.send(object_request)

    def on_task_result(self, task_result: TaskResult):
        # TODO: received result objects should be deleted from the scheduler when no longer needed.
        # This requires to not delete objects that are required by not-yet-computed dependent graph tasks.
        # For now, we just remove the objects when the client makes a clear request, or on client shutdown.
        # https://github.com/Citi/scaler/issues/43

        self._sent_object_ids.update(task_result.results)

    async def clear_all_objects(self, clear_serializer):
        cleared_object_ids = self._sent_object_ids.copy()

        if clear_serializer:
            self._sent_serializer_id = None
        elif self._sent_serializer_id is not None:
            cleared_object_ids.remove(self._sent_serializer_id)

        self._sent_object_ids.difference_update(cleared_object_ids)

        await self._connector_external.send(
            ObjectInstruction.new_msg(
                ObjectInstruction.ObjectInstructionType.Delete,
                self._identity,
                ObjectContent.new_msg(tuple(cleared_object_ids)),
            )
        )

    async def __send_object_creation(self, instruction: ObjectInstruction):
        assert instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create

        new_object_ids = set(instruction.object_content.object_ids) - self._sent_object_ids
        if not new_object_ids:
            return

        if ObjectContent.ObjectContentType.Serializer in instruction.object_content.object_types:
            if self._sent_serializer_id is not None:
                raise ValueError("trying to send multiple serializers.")

            serializer_index = instruction.object_content.object_types.index(ObjectContent.ObjectContentType.Serializer)
            self._sent_serializer_id = instruction.object_content.object_ids[serializer_index]

        new_object_content = ObjectContent.new_msg(
            *zip(
                *filter(
                    lambda object_pack: object_pack[0] in new_object_ids,
                    zip(
                        instruction.object_content.object_ids,
                        instruction.object_content.object_types,
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

        if self._sent_serializer_id in instruction.object_content.object_ids:
            raise ValueError("trying to delete serializer.")

        self._sent_object_ids.difference_update(instruction.object_content.object_ids)

        await self._connector_external.send(instruction)
