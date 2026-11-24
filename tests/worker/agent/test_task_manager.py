import asyncio
import unittest

from scaler.protocol.capnp import Task
from scaler.utility.identifiers import ClientID, TaskID
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker.agent.task_manager import _TaskState, _TaskTable

NEXT_TIMEOUT_SECONDS = 1
LOW_PRIORITY = 0
HIGH_PRIORITY = 3


def _task(name: str, priority: int) -> Task:
    return Task(
        taskId=TaskID(name.encode()),
        source=ClientID(b"client"),
        metadata=TaskFlags(priority=priority).serialize(),
        funcObjectId=b"",
        functionArgs=[],
        capabilities={},
    )


class TestTaskTable(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.table = _TaskTable()

    async def __next_task_id(self) -> TaskID:
        entry = await asyncio.wait_for(self.table.wait_for_next(), NEXT_TIMEOUT_SECONDS)
        return entry.task.taskId

    async def test_the_highest_priority_comes_first(self) -> None:
        self.table.add(_task("low", LOW_PRIORITY), LOW_PRIORITY)
        high = self.table.add(_task("high", HIGH_PRIORITY), HIGH_PRIORITY)
        self.assertEqual(await self.__next_task_id(), high.task.taskId)

    async def test_the_oldest_comes_first_among_equal_priorities(self) -> None:
        first = self.table.add(_task("first", LOW_PRIORITY), LOW_PRIORITY)
        self.table.add(_task("second", LOW_PRIORITY), LOW_PRIORITY)
        self.assertEqual(await self.__next_task_id(), first.task.taskId)

    async def test_a_suspended_task_comes_before_a_waiting_one_of_equal_priority(self) -> None:
        self.table.add(_task("waiting", LOW_PRIORITY), LOW_PRIORITY)
        suspended = self.table.add(_task("suspended", LOW_PRIORITY), LOW_PRIORITY)
        self.table.set_state(suspended, _TaskState.SUSPENDED)
        self.assertEqual(await self.__next_task_id(), suspended.task.taskId)

    async def test_a_waiting_task_of_higher_priority_comes_before_a_suspended_one(self) -> None:
        suspended = self.table.add(_task("suspended", LOW_PRIORITY), LOW_PRIORITY)
        self.table.set_state(suspended, _TaskState.SUSPENDED)
        waiting = self.table.add(_task("waiting", HIGH_PRIORITY), HIGH_PRIORITY)
        self.assertEqual(await self.__next_task_id(), waiting.task.taskId)

    async def test_running_and_removed_tasks_are_not_next(self) -> None:
        running = self.table.add(_task("running", HIGH_PRIORITY), HIGH_PRIORITY)
        self.table.set_state(running, _TaskState.RUNNING)
        removed = self.table.add(_task("removed", HIGH_PRIORITY), HIGH_PRIORITY)
        self.table.remove(removed)
        waiting = self.table.add(_task("waiting", LOW_PRIORITY), LOW_PRIORITY)

        self.assertEqual(await self.__next_task_id(), waiting.task.taskId)
        self.assertEqual(self.table.waiting_count(), 1)
        self.assertEqual(len(self.table), 2)

    async def test_wait_for_next_waits_until_a_task_can_run(self) -> None:
        next_entry = asyncio.ensure_future(self.table.wait_for_next())
        await asyncio.sleep(0.01)
        self.assertFalse(next_entry.done())

        added = self.table.add(_task("added", LOW_PRIORITY), LOW_PRIORITY)
        self.assertIs(await asyncio.wait_for(next_entry, NEXT_TIMEOUT_SECONDS), added)
