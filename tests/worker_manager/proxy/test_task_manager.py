import asyncio
import unittest
from typing import Any, List, Tuple
from unittest.mock import AsyncMock, MagicMock

from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.capnp import (
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    TaskCancelConfirm,
    TaskCancelConfirmType,
    TaskResult,
    TaskResultType,
)
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.mixins import HeartbeatManager
from scaler.worker_manager.proxy.mixins import ExecutionBackend, TaskDeserializer, TaskInputLoader
from scaler.worker_manager.proxy.task_manager import TaskManager
from tests.utility.utility import logging_test_name
from tests.worker_manager.proxy.test_task_actor import SETTLE_IDLE_YIELDS, _make_backend, _make_task, _make_task_cancel

UPLOAD_TIMEOUT_SECONDS = 1.0


class _TaskManagerTestCase(unittest.IsolatedAsyncioTestCase):
    BASE_CONCURRENCY = 1

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.connector_external = AsyncMock(spec=AsyncConnector)
        self.connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        self.heartbeat_manager = MagicMock(spec=HeartbeatManager)
        self.tm = TaskManager(self.BASE_CONCURRENCY, self.backend)
        self.tm.register(self.connector_external, self.connector_storage, self.heartbeat_manager)

    async def _settle(self) -> None:
        """Runs both routines, as WorkerProcess does, until no event or result is left to handle."""
        idle_yields = 0
        while idle_yields < SETTLE_IDLE_YIELDS:
            if not self.tm._actor._events.empty():
                await self.tm.routine()
                idle_yields = 0
            elif not self.tm._results.empty():
                await self.tm.upload_results()
                idle_yields = 0
            else:
                await asyncio.sleep(0)
                idle_yields += 1

    async def _start_task(self, task: Task) -> asyncio.Future:
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.backend.execute = AsyncMock(return_value=future)
        await self.tm.on_task_new(task)
        await self._settle()
        return future

    async def _cancel(self, task_id: TaskID, force: bool) -> None:
        await self.tm.on_cancel_task(_make_task_cancel(task_id, force=force))
        await self._settle()

    def _sent(self) -> List[Any]:
        return [call.args[0] for call in self.connector_external.send.call_args_list]

    def _task_results(self) -> List[TaskResult]:
        return [msg for msg in self._sent() if isinstance(msg, TaskResult)]


class TestTaskManagerRegister(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()

    def test_register_calls_backend_register_with_callable(self) -> None:
        tm = TaskManager(1, self.backend)
        connector_external = AsyncMock(spec=AsyncConnector)
        connector_storage = AsyncMock(spec=AsyncObjectStorageConnector)
        heartbeat_manager = MagicMock(spec=HeartbeatManager)
        tm.register(connector_external, connector_storage, heartbeat_manager)
        self.backend.register.assert_called_once()
        registered_callable = self.backend.register.call_args[0][0]
        self.assertTrue(callable(registered_callable))


class TestTaskManagerResults(_TaskManagerTestCase):
    async def test_success_path_stores_result_and_sends_messages(self) -> None:
        client_id = ClientID.generate_client_id()
        task = _make_task(source=client_id)

        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"serialized_result"
        serializer_id = ObjectID.generate_serializer_object_id(client_id)
        self.tm._serializers[serializer_id] = mock_serializer

        future = await self._start_task(task)
        future.set_result("the_return_value")
        await self._settle()

        mock_serializer.serialize.assert_called_once_with("the_return_value")
        self.connector_storage.set_object.assert_called_once()
        self.assertEqual(self.connector_external.send.call_count, 2)
        obj_instruction = self.connector_external.send.call_args_list[0][0][0]
        self.assertEqual(obj_instruction.instructionType, ObjectInstruction.ObjectInstructionType.create)
        self.assertEqual(obj_instruction.objectUser, client_id)
        task_result = self.connector_external.send.call_args_list[1][0][0]
        self.assertEqual(task_result.resultType, TaskResultType.success)
        self.assertEqual(task_result.taskId, task.taskId)
        self.assertEqual(self.tm.processing_task_count, 0)
        self.backend.on_cleanup.assert_called_once_with(task.taskId)
        self.assertTrue(self.tm.can_accept_task())

    async def test_failure_path_sends_failed_task_result(self) -> None:
        task = _make_task()

        future = await self._start_task(task)
        future.set_exception(RuntimeError("boom"))
        await self._settle()

        self.connector_storage.set_object.assert_called_once()
        (task_result,) = self._task_results()
        self.assertEqual(task_result.resultType, TaskResultType.failed)
        self.assertEqual(task_result.taskId, task.taskId)
        self.backend.on_cleanup.assert_called_once_with(task.taskId)

    async def test_a_new_task_starts_while_a_result_uploads(self) -> None:
        """routine() hands a result to upload_results() rather than awaiting the upload itself."""
        self.connector_storage.set_object = AsyncMock(side_effect=asyncio.Event().wait)
        first = await self._start_task(_make_task())
        first.set_result(None)
        await asyncio.sleep(0)
        await asyncio.wait_for(self.tm.routine(), timeout=UPLOAD_TIMEOUT_SECONDS)

        task = _make_task()
        self.backend.execute = AsyncMock(return_value=asyncio.get_running_loop().create_future())
        await self.tm.on_task_new(task)
        await asyncio.wait_for(self.tm.routine(), timeout=UPLOAD_TIMEOUT_SECONDS)
        await asyncio.sleep(0)

        self.backend.execute.assert_called_once_with(task)


class TestTaskManagerWiring(_TaskManagerTestCase):
    async def test_a_cancel_confirm_reaches_the_scheduler(self) -> None:
        task_id = TaskID.generate_task_id()

        await self._cancel(task_id, force=False)

        (confirm,) = self._sent()
        self.assertIsInstance(confirm, TaskCancelConfirm)
        self.assertEqual(confirm.taskId, task_id)
        self.assertEqual(confirm.cancelConfirmType, TaskCancelConfirmType.cancelNotFound)

    async def test_the_heartbeat_reads_the_actor(self) -> None:
        await self._start_task(_make_task())
        await self.tm.on_task_new(_make_task())
        await self._settle()

        self.assertEqual(self.tm.get_queued_size(), 1)
        self.assertEqual(self.tm.processing_task_count, 1)
        self.assertFalse(self.tm.can_accept_task())


class TestTaskManagerOnObjectInstruction(_TaskManagerTestCase):
    async def test_delete_removes_serializer_from_cache(self) -> None:
        client_id = ClientID.generate_client_id()
        obj_id = ObjectID.generate_object_id(client_id)
        self.tm._serializers[obj_id] = MagicMock()
        instruction = ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.delete,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(objectIds=(obj_id,), objectTypes=(), objectNames=()),
        )
        await self.tm.on_object_instruction(instruction)
        self.assertNotIn(obj_id, self.tm._serializers)

    async def test_delete_unknown_id_does_not_raise(self) -> None:
        client_id = ClientID.generate_client_id()
        obj_id = ObjectID.generate_object_id(client_id)
        instruction = ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.delete,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(objectIds=(obj_id,), objectTypes=(), objectNames=()),
        )
        await self.tm.on_object_instruction(instruction)
        self.assertEqual(len(self.tm._serializers), 0)

    async def test_unknown_instruction_type_logs_error(self) -> None:
        client_id = ClientID.generate_client_id()
        obj_id = ObjectID.generate_object_id(client_id)
        instruction = ObjectInstruction(
            instructionType=ObjectInstruction.ObjectInstructionType.create,
            objectUser=client_id,
            objectMetadata=ObjectMetadata(
                objectIds=(obj_id,), objectTypes=(ObjectMetadata.ObjectContentType.object,), objectNames=(b"name",)
            ),
        )
        with self.assertLogs("scaler", level="ERROR"):
            await self.tm.on_object_instruction(instruction)


class TestExecutionBackendSentinel(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_load_task_inputs_after_register_does_not_raise(self) -> None:
        async def _loader(task: Task) -> Tuple[Any, List[Any]]:
            return None, []

        class _ConcreteBackend(TaskInputLoader, ExecutionBackend):
            _loader: TaskDeserializer

            def register(self, load_task_inputs: TaskDeserializer) -> None:
                self._loader = load_task_inputs

            async def load_task_inputs(self, task: Task) -> Tuple[Any, List[Any]]:
                return await self._loader(task)

            async def execute(self, task: Task) -> asyncio.Future:
                return asyncio.get_running_loop().create_future()

            async def on_cancel(self, task_cancel: TaskCancel) -> None:
                pass

            def on_cleanup(self, task_id: TaskID) -> None:
                pass

            async def routine(self) -> None:
                pass

        backend = _ConcreteBackend()
        backend.register(_loader)
        func, args = await backend.load_task_inputs(_make_task())
        self.assertIsNone(func)
        self.assertEqual(args, [])
