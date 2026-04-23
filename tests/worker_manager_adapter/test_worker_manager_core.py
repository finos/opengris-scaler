import signal
import unittest
from typing import Optional
from unittest.mock import AsyncMock, MagicMock, patch

from scaler.protocol.capnp import (
    ClientDisconnect,
    DisconnectResponse,
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    WorkerHeartbeatEcho,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
)
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerProvisioner
from scaler.worker_manager_adapter.mixins import WorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner
from scaler.worker_manager_adapter.worker_process import WorkerProcess
from tests.utility.utility import logging_test_name

Status = WorkerManagerCommandResponse.Status


class TestWorkerManagerHandleCommand(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.capabilities = {"cpu": 4}
        self.provisioner = MagicMock(spec=WorkerProvisioner)
        self.provisioner.start_worker = AsyncMock()
        self.provisioner.shutdown_workers = AsyncMock()
        self.send_mock = AsyncMock()
        self.runner = WorkerManagerRunner(
            address=MagicMock(),
            name="test_runner",
            heartbeat_interval_seconds=5,
            capabilities=self.capabilities,
            max_task_concurrency=4,
            worker_manager_id=b"mgr",
            worker_provisioner=self.provisioner,
        )
        connector = AsyncMock()
        connector.send = self.send_mock
        self.runner._connector_external = connector

    def _sent_response(self) -> WorkerManagerCommandResponse:
        self.send_mock.assert_called_once()
        return self.send_mock.call_args[0][0]

    async def test_start_workers_success_populates_capabilities_and_worker_ids(self) -> None:
        worker_id = bytes(WorkerID.generate_worker_id("w0"))
        self.provisioner.start_worker.return_value = ([worker_id], Status.success)

        cmd = MagicMock(spec=WorkerManagerCommand)
        cmd.command = WorkerManagerCommandType.startWorkers
        await self.runner._handle_command(cmd)

        response = self._sent_response()
        self.assertIsInstance(response, WorkerManagerCommandResponse)
        self.assertEqual(response.status, Status.success)
        self.assertEqual(list(response.workerIDs), [worker_id])
        self.assertEqual(dict(response.capabilities), self.capabilities)

    async def test_start_workers_failure_returns_empty_capabilities_and_ids(self) -> None:
        self.provisioner.start_worker.return_value = ([], Status.tooManyWorkers)

        cmd = MagicMock(spec=WorkerManagerCommand)
        cmd.command = WorkerManagerCommandType.startWorkers
        await self.runner._handle_command(cmd)

        response = self._sent_response()
        self.assertEqual(response.status, Status.tooManyWorkers)
        self.assertEqual(list(response.workerIDs), [])
        self.assertEqual(dict(response.capabilities), {})

    async def test_shutdown_workers_echoes_ids_with_empty_capabilities(self) -> None:
        worker_ids = [bytes(WorkerID.generate_worker_id(f"w{i}")) for i in range(2)]
        self.provisioner.shutdown_workers.return_value = (worker_ids, Status.success)

        cmd = MagicMock(spec=WorkerManagerCommand)
        cmd.command = WorkerManagerCommandType.shutdownWorkers
        cmd.workerIDs = worker_ids
        await self.runner._handle_command(cmd)

        response = self._sent_response()
        self.assertEqual(response.status, Status.success)
        self.assertEqual(list(response.workerIDs), worker_ids)
        self.assertEqual(dict(response.capabilities), {})
        self.provisioner.shutdown_workers.assert_called_once_with(worker_ids)

    async def test_unknown_command_type_is_silently_ignored(self) -> None:
        cmd = MagicMock(spec=WorkerManagerCommand)
        cmd.command = object()

        await self.runner._handle_command(cmd)

        self.send_mock.assert_not_called()

    async def test_set_desired_task_concurrency_is_silently_ignored(self) -> None:
        cmd = MagicMock(spec=WorkerManagerCommand)
        cmd.command = WorkerManagerCommandType.setDesiredTaskConcurrency

        await self.runner._handle_command(cmd)

        self.send_mock.assert_not_called()
        self.provisioner.start_worker.assert_not_called()
        self.provisioner.shutdown_workers.assert_not_called()


class TestWorkerProcessOnReceiveExternal(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.on_heartbeat_echo = AsyncMock()
        self.on_task_new = AsyncMock()
        self.on_cancel_task = AsyncMock()
        self.on_object_instruction = AsyncMock()
        self.task_cancel = MagicMock()

        heartbeat_manager = MagicMock()
        heartbeat_manager.on_heartbeat_echo = self.on_heartbeat_echo
        task_manager = MagicMock()
        task_manager.on_task_new = self.on_task_new
        task_manager.on_cancel_task = self.on_cancel_task
        task_manager.on_object_instruction = self.on_object_instruction

        self.wp = WorkerProcess(
            name="test_worker",
            address=MagicMock(),
            object_storage_address=None,
            capabilities={},
            base_concurrency=1,
            heartbeat_interval_seconds=1,
            death_timeout_seconds=10,
            task_queue_size=10,
            io_threads=1,
            event_loop="asyncio",
            worker_manager_id=b"mgr",
            processor_status_provider_factory=MagicMock(),
            execution_backend_factory=MagicMock(),
        )
        self.wp._heartbeat_manager = heartbeat_manager
        self.wp._task_manager = task_manager
        self.wp._task = self.task_cancel

    async def _dispatch(self, message: object) -> None:
        await self.wp._WorkerProcess__on_receive_external(message)

    async def test_messages_before_heartbeat_echo_are_queued(self) -> None:
        task = _make_task()
        await self._dispatch(task)

        self.assertEqual(len(self.wp._backoff_message_queue), 1)
        self.assertIs(self.wp._backoff_message_queue[0], task)
        self.on_task_new.assert_not_called()

    async def test_queued_messages_replayed_in_order_after_heartbeat_echo(self) -> None:
        task1 = _make_task()
        task2 = _make_task()
        await self._dispatch(task1)
        await self._dispatch(task2)
        self.assertEqual(len(self.wp._backoff_message_queue), 2)

        echo = WorkerHeartbeatEcho()
        await self._dispatch(echo)

        self.assertTrue(self.wp._heartbeat_received)
        self.assertEqual(len(self.wp._backoff_message_queue), 0)
        self.on_heartbeat_echo.assert_called_once_with(echo)
        calls = self.on_task_new.call_args_list
        self.assertEqual(len(calls), 2)
        self.assertIs(calls[0][0][0], task1)
        self.assertIs(calls[1][0][0], task2)

    async def test_task_routes_to_on_task_new(self) -> None:
        self.wp._heartbeat_received = True
        task = _make_task()
        await self._dispatch(task)
        self.on_task_new.assert_called_once_with(task)

    async def test_task_cancel_routes_to_on_cancel_task(self) -> None:
        self.wp._heartbeat_received = True
        cancel = _make_task_cancel()
        await self._dispatch(cancel)
        self.on_cancel_task.assert_called_once_with(cancel)

    async def test_object_instruction_routes_to_on_object_instruction(self) -> None:
        self.wp._heartbeat_received = True
        instruction = _make_object_instruction()
        await self._dispatch(instruction)
        self.on_object_instruction.assert_called_once_with(instruction)

    async def test_client_disconnect_shutdown_raises_client_shutdown_exception(self) -> None:
        self.wp._heartbeat_received = True
        msg = ClientDisconnect(disconnectType=ClientDisconnect.DisconnectType.shutdown)
        with self.assertRaises(ClientShutdownException):
            await self._dispatch(msg)

    async def test_disconnect_response_cancels_task(self) -> None:
        self.wp._heartbeat_received = True
        msg = DisconnectResponse(worker=WorkerID(b""))
        await self._dispatch(msg)
        self.task_cancel.cancel.assert_called_once()

    async def test_unknown_message_type_raises_type_error(self) -> None:
        self.wp._heartbeat_received = True

        class _Unknown:
            pass

        with self.assertRaises(TypeError):
            await self._dispatch(_Unknown())


class TestNativeWorkerProvisioner(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_start_worker_at_capacity_returns_too_many_workers(self) -> None:
        provisioner = _make_native_provisioner(max_workers=1)
        provisioner._workers[WorkerID.generate_worker_id("existing")] = MagicMock()

        ids, status = await provisioner.start_worker()

        self.assertEqual(ids, [])
        self.assertEqual(status, Status.tooManyWorkers)

    async def test_start_worker_success_starts_and_registers_worker(self) -> None:
        provisioner = _make_native_provisioner(max_workers=2)
        mock_worker = MagicMock()
        mock_worker.identity = WorkerID.generate_worker_id("w0")

        with patch.object(provisioner, "_create_worker", return_value=mock_worker):
            ids, status = await provisioner.start_worker()

        mock_worker.start.assert_called_once()
        self.assertEqual(status, Status.success)
        self.assertEqual(ids, [bytes(mock_worker.identity)])
        self.assertIn(mock_worker.identity, provisioner._workers)

    async def test_shutdown_workers_unknown_id_returns_worker_not_found(self) -> None:
        provisioner = _make_native_provisioner()
        unknown_bytes = bytes(WorkerID.generate_worker_id("ghost"))

        ids, status = await provisioner.shutdown_workers([unknown_bytes])

        self.assertEqual(ids, [])
        self.assertEqual(status, Status.workerNotFound)

    async def test_shutdown_workers_kills_joins_and_returns_id(self) -> None:
        provisioner = _make_native_provisioner()
        worker_id = WorkerID.generate_worker_id("w0")
        mock_worker = MagicMock()
        mock_worker.pid = 99999
        provisioner._workers[worker_id] = mock_worker
        worker_id_bytes = bytes(worker_id)

        with patch("os.kill") as mock_kill:
            ids, status = await provisioner.shutdown_workers([worker_id_bytes])

        mock_kill.assert_called_once_with(99999, signal.SIGINT)
        mock_worker.join.assert_called_once()
        self.assertEqual(ids, [worker_id_bytes])
        self.assertEqual(status, Status.success)
        self.assertNotIn(worker_id, provisioner._workers)


def _make_task(source: Optional[ClientID] = None) -> Task:
    source = source or ClientID.generate_client_id()
    return Task(
        taskId=TaskID.generate_task_id(),
        source=source,
        metadata=TaskFlags(priority=0).serialize(),
        funcObjectId=ObjectID.generate_object_id(source),
        functionArgs=[],
        capabilities={},
    )


def _make_task_cancel() -> TaskCancel:
    return TaskCancel(taskId=TaskID.generate_task_id(), flags=TaskCancel.TaskCancelFlags(force=False))


def _make_object_instruction() -> ObjectInstruction:
    client_id = ClientID.generate_client_id()
    return ObjectInstruction(
        instructionType=ObjectInstruction.ObjectInstructionType.delete,
        objectUser=client_id,
        objectMetadata=ObjectMetadata(
            objectIds=(ObjectID.generate_object_id(client_id),), objectTypes=(), objectNames=()
        ),
    )


def _make_native_provisioner(max_workers: int = 2) -> NativeWorkerProvisioner:
    config = MagicMock()
    config.worker_manager_config.max_task_concurrency = max_workers
    config.worker_manager_config.worker_manager_id.encode.return_value = b"mgr"
    config.worker_type = "NAT"
    return NativeWorkerProvisioner(config)
