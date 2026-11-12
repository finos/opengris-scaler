import asyncio
import multiprocessing
import unittest
from typing import List

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import get_network_backend_from_env
from scaler.protocol.capnp import BaseMessage, WorkerManagerCommand, WorkerManagerHeartbeat, WorkerManagerHeartbeatEcho
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name

UNIT_ID = "unit-under-test"
TIMEOUT_SECONDS = 60

# Long enough for the child to start a worker and run the task, had it been asked for one.
NO_WORKER_WAIT_SECONDS = 5


def _add(a: int, b: int) -> int:
    return a + b


class _FakeParent:
    """Plays a cloud worker manager: asks its one child for `task_concurrency` workers on every heartbeat."""

    def __init__(self, address: AddressConfig) -> None:
        self.address = address
        self.task_concurrency = 0
        self.received: List[str] = []
        self._backend = get_network_backend_from_env(io_threads=1)
        self._binder = self._backend.create_async_binder(identity=b"fake-parent", callback=self._on_receive)

    async def run(self) -> None:
        await self._binder.bind(self.address)
        while True:
            await self._binder.routine()

    def destroy(self) -> None:
        self._binder.destroy()

    async def _on_receive(self, source: bytes, message: BaseMessage) -> None:
        assert source == UNIT_ID.encode(), "the child dials with its unit id as identity"
        self.received.append(type(message).__name__)

        assert isinstance(message, WorkerManagerHeartbeat)
        await self._binder.send(source, WorkerManagerHeartbeatEcho(), detached=True)

        request = WorkerManagerCommand.DesiredTaskConcurrencyRequest(
            taskConcurrency=self.task_concurrency, capabilities=[]
        )
        await self._binder.send(
            source, WorkerManagerCommand(setDesiredTaskConcurrencyRequests=[request]), detached=True
        )


def _child_manager_process(combo: SchedulerClusterCombo, parent: _FakeParent) -> multiprocessing.process.BaseProcess:
    return multiprocessing.get_context("spawn").Process(
        target=NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=AddressConfig.from_string(combo.get_address()),
                    worker_manager_id="nesting-test",
                    max_task_concurrency=1,
                ),
                worker_config=WorkerConfig(heartbeat_interval_seconds=1),
                parent_address=parent.address,
                unit_id=UNIT_ID,
            )
        ).run
    )


class TestChildManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_a_child_manager_runs_the_workers_its_parent_asks_for(self) -> None:
        """The task waits while the parent asks for no worker, and runs once the parent asks for one."""
        combo = SchedulerClusterCombo(n_workers=0)
        parent = _FakeParent(AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"))
        parent_task = asyncio.ensure_future(parent.run())
        child_process = _child_manager_process(combo, parent)
        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(0.5)  # let the fake parent bind before the child dials it
            child_process.start()

            with Client(address=combo.get_address()) as client:
                future = client.submit(_add, 2, 3)
                await asyncio.sleep(NO_WORKER_WAIT_SECONDS)
                self.assertFalse(future.done())

                parent.task_concurrency = 1
                result = await loop.run_in_executor(None, future.result, TIMEOUT_SECONDS)

            self.assertEqual(result, 5)
            self.assertIn("WorkerManagerHeartbeat", parent.received)
        finally:
            if child_process.is_alive():
                child_process.terminate()
                child_process.join()
            parent_task.cancel()
            parent.destroy()
            combo.shutdown()
