import asyncio
import unittest
from typing import List
from unittest.mock import AsyncMock, MagicMock

from scaler.protocol.capnp import Task, TaskCancel
from scaler.utility.identifiers import ClientID, TaskID
from scaler.utility.metadata.task_flags import TaskFlags
from scaler.worker.agent.task_manager import VanillaTaskManager

TASK_TIMEOUT_SECONDS = 10


def _task(name: str) -> Task:
    return Task(
        taskId=TaskID(name.encode()),
        source=ClientID(b"client"),
        metadata=TaskFlags().serialize(),
        funcObjectId=b"",
        functionArgs=[],
        capabilities={},
    )


class TestVanillaTaskManagerDrain(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.started: List[TaskID] = []

        processor_manager = MagicMock()
        processor_manager.wait_until_can_accept_task = AsyncMock()
        processor_manager.current_task.return_value = None
        processor_manager.on_task = AsyncMock(side_effect=lambda task: self.started.append(task.taskId))

        self.task_manager = VanillaTaskManager(task_timeout_seconds=TASK_TIMEOUT_SECONDS)
        self.task_manager.register(connector=AsyncMock(), processor_manager=processor_manager)

    async def test_a_draining_worker_starts_no_queued_task(self) -> None:
        queued = _task("queued")
        await self.task_manager.on_task_new(queued)
        self.task_manager.drain()

        routine = asyncio.ensure_future(self.task_manager.routine())
        await asyncio.sleep(0.01)
        self.assertEqual(self.started, [])
        self.assertFalse(self.task_manager.is_idle())

        await self.task_manager.on_cancel_task(
            TaskCancel(taskId=queued.taskId, flags=TaskCancel.TaskCancelFlags(force=False))
        )
        await asyncio.wait_for(routine, timeout=1)
        self.assertEqual(self.started, [])
        self.assertTrue(self.task_manager.is_idle())

    async def test_a_serving_worker_starts_its_queued_task(self) -> None:
        queued = _task("queued")
        await self.task_manager.on_task_new(queued)
        await asyncio.wait_for(self.task_manager.routine(), timeout=1)
        self.assertEqual(self.started, [queued.taskId])
