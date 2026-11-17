import asyncio
import logging
import unittest
from typing import Optional
from unittest.mock import AsyncMock, MagicMock

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.io.utility import deserialize, serialize
from scaler.protocol.capnp import (
    ClientDisconnect,
    ObjectInstruction,
    ObjectMetadata,
    Task,
    TaskCancel,
    TaskCapability,
    WorkerDisconnectNotification,
    WorkerHeartbeat,
    WorkerHeartbeatEcho,
    WorkerManagerCommand,
    WorkerManagerHeartbeat,
)
from scaler.protocol.helpers import dict_to_capabilities
from scaler.utility.exceptions import ClientShutdownException
from scaler.utility.identifiers import ClientID, ObjectID, TaskID, WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.utility.telemetry import empty_trace_context
from scaler.worker_manager.mixins import UnitProvisioner
from scaler.worker_manager.proxy.worker_process import WorkerProcess
from scaler.worker_manager.runner import WorkerManagerRunner
from tests.utility.utility import logging_test_name

TASK_CONCURRENCY_PER_UNIT = 4


class TestChildManagerTaskConcurrency(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        provisioner = MagicMock(spec=UnitProvisioner)
        provisioner.create_unit = AsyncMock(side_effect=lambda unit_id: unit_id)
        provisioner.max_units.return_value = -1
        provisioner.task_concurrency_per_unit.return_value = TASK_CONCURRENCY_PER_UNIT
        provisioner.startup_timeout_seconds.return_value = 60
        self.runner = WorkerManagerRunner(
            name="test_runner",
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:1"),
                worker_manager_id=WorkerManagerID(b"mgr"),
                scale_down_cooldown_seconds=60,
            ),
            heartbeat_interval_seconds=5,
            capabilities={},
            create_provisioner=lambda _children_address: provisioner,
            children_address=AddressConfig.from_string("tcp://127.0.0.1:2"),
        )
        # _get_loops creates the controller after the bind, which never runs here
        self.runner._create_unit_controller(AddressConfig.from_string("tcp://127.0.0.1:2"))
        self.binder = AsyncMock()
        self.runner._binder_children = self.binder

    async def __child_heartbeat(self, unit_id: str, assigned_tasks: int) -> int:
        """Send a child manager heartbeat from `unit_id`, and return the task concurrency the runner asks of it."""
        heartbeat = WorkerManagerHeartbeat(
            maxTaskConcurrency=TASK_CONCURRENCY_PER_UNIT,
            capabilities=dict_to_capabilities({}),
            workerManagerID=WorkerManagerID(unit_id.encode()),
            activeTaskConcurrency=TASK_CONCURRENCY_PER_UNIT,
            assignedTasks=assigned_tasks,
            activeUnits=0,
            pendingUnits=0,
        )
        self.binder.send.reset_mock()
        await self.runner._on_receive_child(unit_id.encode(), deserialize(serialize(heartbeat)))
        (command,) = [
            call.args[1] for call in self.binder.send.call_args_list if isinstance(call.args[1], WorkerManagerCommand)
        ]
        return command.setDesiredTaskConcurrencyRequests[0].taskConcurrency

    async def test_every_child_manager_runs_a_whole_unit_while_the_fleet_shrinks(self) -> None:
        """A unit the shed keeps never drains first: every child is asked for the task concurrency of a whole unit."""
        controller = self.runner._unit_controller
        controller.set_desired_task_concurrency(3 * TASK_CONCURRENCY_PER_UNIT)
        await controller.routine()
        await asyncio.sleep(0)
        first, second, busiest = list(controller._units)
        for unit_id in (first, second):
            await self.__child_heartbeat(unit_id, assigned_tasks=0)
        await self.__child_heartbeat(busiest, assigned_tasks=TASK_CONCURRENCY_PER_UNIT)

        controller.set_desired_task_concurrency(TASK_CONCURRENCY_PER_UNIT // 2)
        await controller.routine()

        for unit_id in (first, second, busiest):
            self.assertEqual(await self.__child_heartbeat(unit_id, assigned_tasks=0), TASK_CONCURRENCY_PER_UNIT)


class TestWorkerManagerHandleCommand(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.provisioner = MagicMock(spec=UnitProvisioner)
        self.provisioner.max_units.return_value = 4
        self.provisioner.task_concurrency_per_unit.return_value = 1
        self.send_mock = AsyncMock()
        self.runner = WorkerManagerRunner(
            name="test_runner",
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string("tcp://127.0.0.1:1"),
                worker_manager_id=WorkerManagerID(b"mgr"),
            ),
            heartbeat_interval_seconds=5,
            capabilities={"cpu": 4},
            create_provisioner=lambda _children_address: self.provisioner,
            children_address=AddressConfig.from_string("tcp://127.0.0.1:2"),
        )
        connector = AsyncMock()
        connector.send = self.send_mock
        self.runner._connector_parent = connector
        # _get_loops creates the controller after the bind, which never runs here
        self.runner._create_unit_controller(AddressConfig.from_string("tcp://127.0.0.1:2"))

    async def test_command_sets_the_matching_desired_task_concurrency(self) -> None:
        cmd = WorkerManagerCommand(
            setDesiredTaskConcurrencyRequests=[
                WorkerManagerCommand.DesiredTaskConcurrencyRequest(taskConcurrency=3, capabilities=[]),
                WorkerManagerCommand.DesiredTaskConcurrencyRequest(
                    taskConcurrency=5, capabilities=[TaskCapability(name="gpu", value=-1)]
                ),
            ],
            traceContext=empty_trace_context(),
        )

        await self.runner._handle_command(cmd)

        self.assertEqual(self.runner._unit_controller._desired_task_concurrency, 3)
        self.send_mock.assert_not_called()

    async def test_unknown_command_payload_logs_warning_without_crashing(self) -> None:
        cmd = MagicMock(spec=WorkerManagerCommand)
        # Remove the only recognized payload field to simulate an unknown variant from a
        # newer scheduler (or a remote worker manager) that this runner does not understand.
        del cmd.setDesiredTaskConcurrencyRequests

        with self.assertLogs("scaler", level=logging.WARNING) as captured:
            await self.runner._handle_command(cmd)

        self.assertTrue(any("Unknown action" in m for m in captured.output))
        self.assertEqual(self.runner._unit_controller._desired_task_concurrency, 0)
        self.send_mock.assert_not_called()

    async def test_unknown_message_type_logs_warning_without_crashing(self) -> None:
        class _Unknown:
            pass

        with self.assertLogs("scaler", level=logging.WARNING) as captured:
            await self.runner._on_receive_parent(_Unknown())  # type: ignore[arg-type]

        self.assertTrue(any("Unknown action" in m or "unrecognized" in m for m in captured.output))
        self.send_mock.assert_not_called()

    async def test_a_disconnect_notification_from_a_unit_reaches_the_controller(self) -> None:
        self.runner._unit_controller.on_unit_disconnect = MagicMock()  # type: ignore[method-assign]

        await self.runner._on_receive_child(b"unit-1", WorkerDisconnectNotification())

        self.runner._unit_controller.on_unit_disconnect.assert_called_once_with("unit-1")

    async def test_a_worker_heartbeat_gets_an_echo(self) -> None:
        binder = AsyncMock()
        self.runner._binder_children = binder

        heartbeat = deserialize(serialize(WorkerHeartbeat()))  # a field read needs a struct off the wire
        await self.runner._on_receive_child(b"unit-1", heartbeat)

        binder.send.assert_awaited_once()
        self.assertEqual(binder.send.call_args.args[0], b"unit-1")
        self.assertIsInstance(binder.send.call_args.args[1], WorkerHeartbeatEcho)


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
            worker_manager_id=WorkerManagerID(b"mgr"),
            worker_manager_address=MagicMock(),
            unit_id="unit",
            telemetry_relay=False,
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

    async def test_unknown_message_type_raises_type_error(self) -> None:
        self.wp._heartbeat_received = True

        class _Unknown:
            pass

        with self.assertRaises(TypeError):
            await self._dispatch(_Unknown())


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
