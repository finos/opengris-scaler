import asyncio
import unittest
from typing import List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import ClientID, TaskID
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker.agent.task_manager import VanillaTaskManager

TASK_TIMEOUT_SECONDS = 10
ROUTINE_TIMEOUT_SECONDS = 1
SETTLE_SECONDS = 0.01  # long enough for a routine that is not blocked to run
LOW_PRIORITY = 0
HIGH_PRIORITY = 3


def _task(name: str, priority: int = LOW_PRIORITY) -> Task:
    return Task(
        taskId=TaskID(name.encode()),
        source=ClientID(b"client"),
        metadata=TaskFlags(priority=priority).serialize(),
        funcObjectId=b"",
        functionArgs=[],
        capabilities={},
    )


class TestVanillaTaskManagerDrain(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.started: List[TaskID] = []
        self.resumed: List[TaskID] = []
        self.current_task: Optional[Task] = None

        # The processor slot is a real lock, as in VanillaProcessorManager: a routine that keeps it blocks the next one.
        self.task_slot = asyncio.Lock()
        processor_manager = MagicMock()
        processor_manager.wait_until_can_accept_task = self.task_slot.acquire
        processor_manager.current_task.side_effect = lambda: self.current_task
        processor_manager.on_task = AsyncMock(side_effect=self.__start)
        processor_manager.on_suspend_task = AsyncMock(side_effect=self.__suspend)
        processor_manager.on_resume_task = AsyncMock(side_effect=self.resumed.append)

        self.task_manager = VanillaTaskManager(task_timeout_seconds=TASK_TIMEOUT_SECONDS)
        self.task_manager.register(connector=AsyncMock(), processor_manager=processor_manager)

    def __start(self, task: Task) -> None:
        self.started.append(task.taskId)
        self.current_task = task

    def __suspend(self, task_id: TaskID) -> None:
        # The processor manager starts a fresh processor, which frees the slot once it is initialized.
        self.current_task = None
        self.task_slot.release()

    async def __routine(self) -> None:
        await asyncio.wait_for(self.task_manager.routine(), ROUTINE_TIMEOUT_SECONDS)

    def __start_routine(self) -> asyncio.Task:
        routine = asyncio.ensure_future(self.task_manager.routine())
        self.addCleanup(routine.cancel)
        return routine

    async def __cancel(self, task: Task) -> None:
        await self.task_manager.on_cancel_task(
            TaskCancel(taskId=task.taskId, flags=TaskCancel.TaskCancelFlags(force=False))
        )

    async def __suspend_low_under_high(self) -> Tuple[Task, Task]:
        """Run a low-priority task, then suspend it for a high-priority one, which stays waiting."""
        low, high = _task("low", LOW_PRIORITY), _task("high", HIGH_PRIORITY)
        await self.task_manager.on_task_new(low)
        await self.__routine()
        await self.task_manager.on_task_new(high)
        return low, high

    async def test_a_draining_worker_starts_no_waiting_task(self) -> None:
        waiting = _task("waiting")
        await self.task_manager.on_task_new(waiting)
        self.task_manager.drain()

        routine = self.__start_routine()
        await asyncio.sleep(SETTLE_SECONDS)
        self.assertFalse(routine.done())
        self.assertFalse(self.task_manager.is_idle())

        await self.__cancel(waiting)
        self.assertEqual(self.started, [])
        self.assertTrue(self.task_manager.is_idle())

    async def test_a_serving_worker_starts_its_waiting_task(self) -> None:
        waiting = _task("waiting")
        await self.task_manager.on_task_new(waiting)
        await self.__routine()
        self.assertEqual(self.started, [waiting.taskId])

    async def test_a_draining_worker_resumes_its_suspended_task_ahead_of_a_waiting_one(self) -> None:
        low, high = await self.__suspend_low_under_high()
        self.task_manager.drain()

        await self.__routine()
        self.assertEqual(self.resumed, [low.taskId])
        self.assertEqual(self.started, [low.taskId], "the waiting task never starts")

    async def test_a_task_that_arrives_while_a_draining_worker_waits_for_the_slot_does_not_start(self) -> None:
        low, high = await self.__suspend_low_under_high()
        await self.__cancel(high)
        self.task_manager.drain()

        await self.task_slot.acquire()  # a task still runs: the routine waits for the slot
        routine = self.__start_routine()
        await asyncio.sleep(SETTLE_SECONDS)
        await self.task_manager.on_task_new(_task("late", HIGH_PRIORITY))
        self.task_slot.release()
        await asyncio.wait_for(routine, ROUTINE_TIMEOUT_SECONDS)

        self.assertEqual(self.resumed, [low.taskId])
        self.assertEqual(self.started, [low.taskId], "the late task never starts")

    async def test_a_drain_that_starts_while_the_routine_waits_for_work_holds_back_new_tasks(self) -> None:
        routine = self.__start_routine()
        await asyncio.sleep(SETTLE_SECONDS)  # the routine holds the slot and waits for a task
        self.task_manager.drain()

        await self.task_manager.on_task_new(_task("late"))
        await asyncio.sleep(SETTLE_SECONDS)
        self.assertFalse(routine.done())
        self.assertEqual(self.started, [])
