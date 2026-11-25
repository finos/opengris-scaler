import asyncio
import unittest
from typing import List, Optional

from scaler import SchedulerClusterCombo
from scaler.config.defaults import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_PATHS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
)
from scaler.config.types.address import AddressConfig, SocketType
from scaler.io.network_backends import get_network_backend_from_env
from scaler.protocol.capnp import BaseMessage, WorkerDisconnectNotification, WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import OS_PICKED_PORT
from scaler.worker.worker import Worker
from tests.utility.utility import logging_test_name

UNIT_ID = "unit-under-test"
FAILING_PRELOAD = "tests.utility.utility:failing_preload()"
TIMEOUT_SECONDS = 60


class _FakeManager:
    """Plays a worker manager: echoes each heartbeat of its one worker, and records what the worker sends."""

    def __init__(self, address: AddressConfig) -> None:
        self.address = address
        self.received: List[str] = []
        self.heartbeat = asyncio.Event()
        self.disconnected = asyncio.Event()
        self._backend = get_network_backend_from_env(io_threads=1)
        self._binder = self._backend.create_async_binder(identity=b"fake-manager", callback=self._on_receive)

    async def bind(self) -> None:
        await self._binder.bind(self.address)
        assert self._binder.address is not None
        self.address = self._binder.address

    async def run(self) -> None:
        while True:
            await self._binder.routine()

    def destroy(self) -> None:
        self._binder.destroy()

    async def _on_receive(self, source: bytes, message: BaseMessage) -> None:
        assert source == UNIT_ID.encode(), "the worker dials with its unit id as identity"
        self.received.append(type(message).__name__)

        if isinstance(message, WorkerHeartbeat):
            await self._binder.send(source, WorkerHeartbeatEcho(), detached=True)
            self.heartbeat.set()
        elif isinstance(message, WorkerDisconnectNotification):
            self.disconnected.set()


def _worker(combo: SchedulerClusterCombo, manager: _FakeManager, preload: Optional[str] = None) -> Worker:
    return Worker(
        event_loop="builtin",
        name="manager_link",
        address=AddressConfig.from_string(combo.get_address()),
        object_storage_address=None,
        preload=preload,
        capabilities={},
        io_threads=DEFAULT_IO_THREADS,
        task_queue_size=DEFAULT_PER_WORKER_QUEUE_SIZE,
        heartbeat_interval_seconds=1,
        garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
        death_timeout_seconds=TIMEOUT_SECONDS,
        hard_processor_suspend=False,
        logging_paths=DEFAULT_LOGGING_PATHS,
        logging_level=DEFAULT_LOGGING_LEVEL,
        worker_manager_id=WorkerManagerID(b"fake-manager"),
        worker_manager_address=manager.address,
        unit_id=UNIT_ID,
    )


class TestWorkerManagerLink(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_a_worker_heartbeats_its_manager_and_reports_its_exit(self) -> None:
        combo = SchedulerClusterCombo(n_workers=0)
        manager = _FakeManager(AddressConfig(SocketType.tcp, "127.0.0.1", OS_PICKED_PORT))
        await manager.bind()  # the worker dials the address the manager bound
        manager_task = asyncio.ensure_future(manager.run())
        worker = _worker(combo, manager)
        loop = asyncio.get_running_loop()
        try:
            worker.start()
            await asyncio.wait_for(manager.heartbeat.wait(), TIMEOUT_SECONDS)

            # Closing the scheduler ends the worker through its teardown on every platform; terminate() would not on
            # Windows, where it is TerminateProcess.
            await loop.run_in_executor(None, combo.shutdown)
            await asyncio.wait_for(manager.disconnected.wait(), TIMEOUT_SECONDS)
            await loop.run_in_executor(None, worker.join, TIMEOUT_SECONDS)
        finally:
            if worker.is_alive():
                worker.kill()
                worker.join()
            manager_task.cancel()
            manager.destroy()
            combo.shutdown()

    async def test_a_worker_exits_when_its_processor_dies_before_initializing(self) -> None:
        """A processor that dies at startup ends its worker with exit code 1, instead of leaving it idle forever."""
        combo = SchedulerClusterCombo(n_workers=0)
        manager = _FakeManager(AddressConfig(SocketType.tcp, "127.0.0.1", OS_PICKED_PORT))
        await manager.bind()
        manager_task = asyncio.ensure_future(manager.run())
        worker = _worker(combo, manager, preload=FAILING_PRELOAD)
        loop = asyncio.get_running_loop()
        try:
            worker.start()
            await asyncio.wait_for(manager.disconnected.wait(), TIMEOUT_SECONDS)
            await loop.run_in_executor(None, worker.join, TIMEOUT_SECONDS)
            self.assertEqual(worker.exitcode, 1)
        finally:
            if worker.is_alive():
                worker.kill()
                worker.join()
            manager_task.cancel()
            manager.destroy()
            combo.shutdown()
