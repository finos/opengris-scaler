from typing import Optional, Set

from scaler.client.agent.mixins import ObjectManager
from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.common import ObjectMetadata
from scaler.protocol.python.message import ObjectInstruction, TaskResult
from scaler.utility.identifiers import ClientID, ObjectID


class ClientObjectManager(ObjectManager):
    def __init__(self, identity: ClientID):
        self._sent_object_ids: Set[ObjectID] = set()
        self._sent_serializer_id: Optional[ObjectID] = None

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

    def on_task_result(self, task_result: TaskResult):
        self._sent_object_ids.update((ObjectID(object_id_bytes) for object_id_bytes in task_result.results))

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
                ObjectMetadata.new_msg(tuple(cleared_object_ids)),
            )
        )

    async def __send_object_creation(self, instruction: ObjectInstruction):
        assert instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Create

        new_object_ids = set(instruction.object_metadata.object_ids) - self._sent_object_ids
        if not new_object_ids:
            return

        if ObjectMetadata.ObjectContentType.Serializer in instruction.object_metadata.object_types:
            if self._sent_serializer_id is not None:
                raise ValueError("trying to send multiple serializers.")

            serializer_index = instruction.object_metadata.object_types.index(
                ObjectMetadata.ObjectContentType.Serializer
            )
            self._sent_serializer_id = instruction.object_metadata.object_ids[serializer_index]

        new_object_content = ObjectMetadata.new_msg(
            *zip(
                *filter(
                    lambda object_pack: object_pack[0] in new_object_ids,
                    zip(
                        instruction.object_metadata.object_ids,
                        instruction.object_metadata.object_types,
                        instruction.object_metadata.object_names,
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

        if self._sent_serializer_id in instruction.object_metadata.object_ids:
            raise ValueError("trying to delete serializer.")

        self._sent_object_ids.difference_update(instruction.object_metadata.object_ids)

        await self._connector_external.send(instruction)
