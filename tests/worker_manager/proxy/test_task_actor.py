import asyncio
import unittest
from typing import List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import Task, TaskCancel, TaskCancelConfirmType
from scaler.utility.exceptions import TaskCancelUnsupportedError
from scaler.utility.identifiers import ClientID, ObjectID, TaskID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker_manager.proxy.mixins import ExecutionBackend
from scaler.worker_manager.proxy.task_actor import TaskActor
from tests.utility.utility import logging_test_name

# Yields with nothing to handle before _settle stops: a background execute() or on_cancel() posts within two.
SETTLE_IDLE_YIELDS = 3


def _make_task(priority: int = 0, source: Optional[ClientID] = None, task_id: Optional[TaskID] = None) -> Task:
    source = source or ClientID.generate_client_id()
    task_id = task_id or TaskID.generate_task_id()
    return Task(
        taskId=task_id,
        source=source,
        metadata=TaskFlags(priority=priority).serialize(),
        funcObjectId=ObjectID.generate_object_id(source),
        functionArgs=[],
        capabilities={},
    )


def _make_task_cancel(task_id: TaskID, force: bool = False) -> TaskCancel:
    return TaskCancel(taskId=task_id, flags=TaskCancel.TaskCancelFlags(force=force))


def _make_backend() -> MagicMock:
    backend = MagicMock(spec=ExecutionBackend)
    backend.execute = AsyncMock(side_effect=lambda _: asyncio.get_running_loop().create_future())
    backend.on_cancel = AsyncMock()
    backend.register = MagicMock()
    backend.on_cleanup = MagicMock()
    return backend


class _TaskActorTestCase(unittest.IsolatedAsyncioTestCase):
    BASE_CONCURRENCY = 1

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.backend = _make_backend()
        self.confirms: List[Tuple[TaskID, TaskCancelConfirmType]] = []
        self.reported: List[Tuple[Task, asyncio.Future]] = []

        async def send_cancel_confirm(task_id: TaskID, confirm_type: TaskCancelConfirmType) -> None:
            self.confirms.append((task_id, confirm_type))

        self.actor = TaskActor(
            base_concurrency=self.BASE_CONCURRENCY,
            execution_backend=self.backend,
            send_cancel_confirm=send_cancel_confirm,
            report_result=lambda task, future: self.reported.append((task, future)),
        )

    async def _settle(self) -> None:
        """Runs the actor's routine, as WorkerProcess does, until no event is left to handle."""
        idle_yields = 0
        while idle_yields < SETTLE_IDLE_YIELDS:
            if not self.actor._events.empty():
                await self.actor.routine()
                idle_yields = 0
            else:
                await asyncio.sleep(0)
                idle_yields += 1

    async def _start_task(self, task: Task) -> asyncio.Future:
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.backend.execute = AsyncMock(return_value=future)
        self.actor.post_task_new(task)
        await self._settle()
        return future

    async def _start_task_with_slow_execute(self, task: Task) -> Tuple[asyncio.Event, asyncio.Future]:
        release = asyncio.Event()
        future: asyncio.Future = asyncio.get_running_loop().create_future()

        async def slow_execute(_: Task) -> asyncio.Future:
            await release.wait()
            return future

        self.backend.execute = AsyncMock(side_effect=slow_execute)
        self.actor.post_task_new(task)
        await self._settle()
        return release, future

    async def _cancel(self, task_id: TaskID, force: bool) -> None:
        self.actor.post_cancel(_make_task_cancel(task_id, force=force))
        await self._settle()

    def _cancel_confirm_types(self) -> List[TaskCancelConfirmType]:
        return [confirm_type for _, confirm_type in self.confirms]


class TestTaskActorInit(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def _make_actor(self, base_concurrency: int) -> TaskActor:
        return TaskActor(base_concurrency, _make_backend(), send_cancel_confirm=AsyncMock(), report_result=MagicMock())

    def test_negative_concurrency_raises(self) -> None:
        with self.assertRaises(ValueError):
            self._make_actor(0)
        with self.assertRaises(ValueError):
            self._make_actor(-1)

    def test_valid_concurrency_empty_state(self) -> None:
        actor = self._make_actor(2)
        self.assertEqual(len(actor._task_id_to_entry), 0)
        self.assertEqual(actor.processing_task_count, 0)
        self.assertEqual(actor.queued_task_count, 0)
        self.assertTrue(actor.has_free_permit)


class TestTaskActorConcurrency(_TaskActorTestCase):
    async def test_a_task_starts_when_a_permit_is_free(self) -> None:
        task = _make_task()
        await self._start_task(task)

        self.backend.execute.assert_called_once_with(task)
        self.assertEqual(self.actor.processing_task_count, 1)
        self.assertEqual(self.actor.queued_task_count, 0)
        self.assertFalse(self.actor.has_free_permit)

    async def test_base_concurrency_limits_tasks_of_equal_priority(self) -> None:
        for _ in range(5):
            self.actor.post_task_new(_make_task())
        await self._settle()

        self.assertEqual(self.backend.execute.await_count, 1)
        self.assertEqual(self.actor.queued_task_count, 4)

    async def test_a_queued_task_starts_when_a_task_finishes(self) -> None:
        first = await self._start_task(_make_task())
        queued = _make_task()
        self.actor.post_task_new(queued)
        await self._settle()

        first.set_result(None)
        await self._settle()

        self.backend.execute.assert_called_with(queued)
        self.assertEqual(self.actor.queued_task_count, 0)

    async def test_priority_bypass_strictly_higher(self) -> None:
        await self._start_task(_make_task(priority=1))

        new_task = _make_task(priority=5)
        self.backend.execute = AsyncMock(return_value=asyncio.get_running_loop().create_future())
        self.actor.post_task_new(new_task)
        await self._settle()

        self.backend.execute.assert_called_with(new_task)
        self.assertEqual(self.actor.processing_task_count, 2)
        self.assertEqual(self.actor.queued_task_count, 0)

    async def test_equal_priority_does_not_bypass(self) -> None:
        await self._start_task(_make_task(priority=3))

        self.actor.post_task_new(_make_task(priority=3))
        await self._settle()

        self.backend.execute.assert_called_once()
        self.assertEqual(self.actor.processing_task_count, 1)
        self.assertEqual(self.actor.queued_task_count, 1)

    async def test_lower_priority_does_not_bypass(self) -> None:
        await self._start_task(_make_task(priority=5))

        self.actor.post_task_new(_make_task(priority=2))
        await self._settle()

        self.backend.execute.assert_called_once()
        self.assertEqual(self.actor.processing_task_count, 1)
        self.assertEqual(self.actor.queued_task_count, 1)

    async def test_a_finished_task_is_reported_and_cleaned_up(self) -> None:
        task = _make_task()
        future = await self._start_task(task)

        future.set_result("the_return_value")
        await self._settle()

        self.assertEqual(self.reported, [(task, future)])
        self.backend.on_cleanup.assert_called_once_with(task.taskId)
        self.assertEqual(self.actor.processing_task_count, 0)
        self.assertTrue(self.actor.has_free_permit)

    async def test_a_task_whose_execute_raises_fails_and_gives_back_its_permit(self) -> None:
        task = _make_task()
        self.backend.execute = AsyncMock(side_effect=RuntimeError("cannot submit"))

        with self.assertLogs("scaler", level="ERROR"):
            self.actor.post_task_new(task)
            await self._settle()

        ((reported_task, future),) = self.reported
        self.assertEqual(reported_task, task)
        self.assertIsInstance(future.exception(), RuntimeError)
        self.assertTrue(self.actor.has_free_permit)


class TestTaskActorCancel(_TaskActorTestCase):
    async def test_cancel_nonexistent_sends_cancel_not_found(self) -> None:
        await self._cancel(TaskID.generate_task_id(), force=False)

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.cancelNotFound])

    async def test_cancel_queued_task_clears_queue_and_sends_canceled(self) -> None:
        await self._start_task(_make_task())
        queued = _make_task()
        self.actor.post_task_new(queued)
        await self._settle()

        await self._cancel(queued.taskId, force=False)

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])
        self.assertEqual(self.actor.queued_task_count, 0)
        self.assertNotIn(queued.taskId, self.actor._task_id_to_entry)

    async def test_cancel_processing_without_force_sends_cancel_failed(self) -> None:
        task = _make_task()
        future = await self._start_task(task)

        await self._cancel(task.taskId, force=False)

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.cancelFailed])
        self.assertEqual(self.actor.processing_task_count, 1)
        self.assertFalse(future.cancelled())

    async def test_cancel_processing_with_force_cancels_future_and_sends_canceled(self) -> None:
        task = _make_task()
        future = await self._start_task(task)

        await self._cancel(task.taskId, force=True)

        self.assertTrue(future.cancelled())
        self.backend.on_cancel.assert_called_once()
        self.assertEqual(self.backend.on_cancel.call_args.args[0].taskId, task.taskId)
        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])
        self.assertEqual(self.reported, [])
        self.backend.on_cleanup.assert_called_once_with(task.taskId)
        self.assertTrue(self.actor.has_free_permit)

    async def test_the_next_task_runs_after_a_force_cancel(self) -> None:
        cancelled = _make_task()
        await self._start_task(cancelled)
        await self._cancel(cancelled.taskId, force=True)

        task = _make_task()
        await self._start_task(task)

        self.backend.execute.assert_called_once_with(task)

    async def test_a_force_cancel_survives_a_slow_backend_cancel(self) -> None:
        task = _make_task()
        await self._start_task(task)
        backend_release = asyncio.Event()

        async def slow_on_cancel(_: TaskCancel) -> None:
            await backend_release.wait()

        self.backend.on_cancel = AsyncMock(side_effect=slow_on_cancel)
        await self._cancel(task.taskId, force=True)
        backend_release.set()
        await self._settle()

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])
        self.assertEqual(self.reported, [])
        self.assertTrue(self.actor.has_free_permit)

    async def test_a_force_cancel_is_confirmed_once_the_backend_cancel_returns(self) -> None:
        task = _make_task()
        await self._start_task(task)
        backend_release = asyncio.Event()

        async def slow_on_cancel(_: TaskCancel) -> None:
            await backend_release.wait()

        self.backend.on_cancel = AsyncMock(side_effect=slow_on_cancel)
        await self._cancel(task.taskId, force=True)
        self.assertEqual(self._cancel_confirm_types(), [])

        backend_release.set()
        await self._settle()
        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])

    async def test_a_failed_backend_cancel_is_confirmed_as_failed_and_the_task_reports_its_result(self) -> None:
        task = _make_task()
        future = await self._start_task(task)
        self.backend.on_cancel = AsyncMock(side_effect=RuntimeError("terminate_job failed"))

        with self.assertLogs("scaler", level="ERROR"):
            await self._cancel(task.taskId, force=True)

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.cancelFailed])
        self.assertFalse(future.cancelled())
        self.assertFalse(self.actor.has_free_permit)

        future.set_result("done remotely")
        await self._settle()
        self.assertEqual(self.reported, [(task, future)])
        self.assertTrue(self.actor.has_free_permit)

    async def test_an_unsupported_backend_cancel_logs_one_warning_and_the_task_reports_its_result(self) -> None:
        """A backend that cannot stop a task is not a fault, so the failed cancel is a warning without a traceback."""
        task = _make_task()
        future = await self._start_task(task)
        self.backend.on_cancel = AsyncMock(side_effect=TaskCancelUnsupportedError("the backend cannot stop a task"))

        with self.assertLogs("scaler", level="WARNING") as captured:
            await self._cancel(task.taskId, force=True)

        self.assertEqual([(record.levelname, record.exc_info) for record in captured.records], [("WARNING", None)])
        self.assertIn("the backend cannot stop a task", captured.records[0].getMessage())
        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.cancelFailed])

        future.set_result("done remotely")
        await self._settle()
        self.assertEqual(self.reported, [(task, future)])

    async def test_a_task_that_ends_while_a_backend_cancel_fails_reads_as_canceled(self) -> None:
        """The task was dropped unreported when it ended, so cancelFailed would leave the scheduler waiting forever."""
        task = _make_task()
        future = await self._start_task(task)

        async def on_cancel_while_job_finishes(_: TaskCancel) -> None:
            future.set_result("done remotely")
            await asyncio.sleep(0)
            raise RuntimeError("terminate_job failed")

        self.backend.on_cancel = AsyncMock(side_effect=on_cancel_while_job_finishes)
        with self.assertLogs("scaler", level="ERROR"):
            await self._cancel(task.taskId, force=True)

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])
        self.assertEqual(self.reported, [])
        self.assertTrue(self.actor.has_free_permit)

    async def test_a_task_that_finishes_during_the_backend_cancel_reports_nothing(self) -> None:
        task = _make_task()
        future = await self._start_task(task)

        async def on_cancel_while_job_finishes(_: TaskCancel) -> None:
            future.set_result("done remotely")

        self.backend.on_cancel = AsyncMock(side_effect=on_cancel_while_job_finishes)
        await self._cancel(task.taskId, force=True)

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])
        self.assertEqual(self.reported, [])
        self.assertTrue(self.actor.has_free_permit)

    async def test_a_cancel_before_execute_returns_is_confirmed_and_reaches_the_backend(self) -> None:
        task = _make_task()
        release, future = await self._start_task_with_slow_execute(task)

        await self._cancel(task.taskId, force=True)
        release.set()
        await self._settle()

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.canceled])
        self.backend.on_cancel.assert_called_once()
        self.assertEqual(self.backend.on_cancel.call_args.args[0].taskId, task.taskId)
        self.assertTrue(future.cancelled())
        self.assertEqual(self.reported, [])
        self.assertTrue(self.actor.has_free_permit)

    async def test_a_non_force_cancel_before_execute_returns_fails(self) -> None:
        task = _make_task()
        release, _ = await self._start_task_with_slow_execute(task)

        await self._cancel(task.taskId, force=False)
        release.set()
        await self._settle()

        self.assertEqual(self._cancel_confirm_types(), [TaskCancelConfirmType.cancelFailed])
        self.backend.on_cancel.assert_not_called()
        self.assertEqual(self.actor.processing_task_count, 1)

    async def test_a_second_force_cancel_is_not_found(self) -> None:
        task = _make_task()
        release, _ = await self._start_task_with_slow_execute(task)

        await self._cancel(task.taskId, force=True)
        await self._cancel(task.taskId, force=True)
        release.set()
        await self._settle()

        expected = [TaskCancelConfirmType.cancelNotFound, TaskCancelConfirmType.canceled]
        self.assertEqual(self._cancel_confirm_types(), expected)
        self.backend.on_cancel.assert_called_once()


class TestTaskActorGetTaskPriority(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    def test_negative_priority_raises(self) -> None:
        task = _make_task(priority=-1)
        with self.assertRaises(ValueError):
            TaskActor._get_task_priority(task)

    def test_zero_priority_returns_zero(self) -> None:
        self.assertEqual(TaskActor._get_task_priority(_make_task(priority=0)), 0)

    def test_positive_priority_returns_value(self) -> None:
        self.assertEqual(TaskActor._get_task_priority(_make_task(priority=7)), 7)

    def test_empty_metadata_returns_zero(self) -> None:
        source = ClientID.generate_client_id()
        task = Task(
            taskId=TaskID.generate_task_id(),
            source=source,
            metadata=b"",
            funcObjectId=ObjectID.generate_object_id(source),
            functionArgs=[],
            capabilities={},
        )
        self.assertEqual(TaskActor._get_task_priority(task), 0)
