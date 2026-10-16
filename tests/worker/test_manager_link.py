import asyncio
import unittest
from typing import List

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
from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import get_network_backend_from_env
from scaler.protocol.capnp import BaseMessage, WorkerDisconnectNotification, WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker.worker import Worker
from tests.utility.utility import logging_test_name

UNIT_ID = "unit-under-test"
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

    async def run(self) -> None:
        await self._binder.bind(self.address)
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


def _worker(combo: SchedulerClusterCombo, manager: _FakeManager) -> Worker:
    return Worker(
        event_loop="builtin",
        name="manager_link",
        address=AddressConfig.from_string(combo.get_address()),
        object_storage_address=None,
        preload=None,
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
        worker_manager_id=b"fake-manager",
        worker_manager_address=manager.address,
        unit_id=UNIT_ID,
    )


class TestWorkerManagerLink(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_a_worker_heartbeats_its_manager_and_reports_its_exit(self) -> None:
        combo = SchedulerClusterCombo(n_workers=0)
        manager = _FakeManager(AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"))
        manager_task = asyncio.ensure_future(manager.run())
        worker = _worker(combo, manager)
        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(0.5)  # let the fake manager bind before the worker dials it
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
