import asyncio
import multiprocessing
import os
import tempfile
import time
import unittest
from typing import List

from scaler import Client, SchedulerClusterCombo
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.io.network_backends import get_network_backend_from_env
from scaler.protocol.capnp import (
    BaseMessage,
    WorkerManagerCommand,
    WorkerManagerDisconnectNotification,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
    WorkerManagerShutdown,
)
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.utility import logging_test_name

UNIT_ID = "unit-under-test"
TASK_SECONDS = 5
TIMEOUT_SECONDS = 60


def _record_start_then_sleep(start_log_path: str, seconds: int) -> int:
    with open(start_log_path, "a") as start_log:
        start_log.write(f"{os.getpid()}\n")
    time.sleep(seconds)
    return seconds


class _FakeParent:
    """Plays a provisioner manager: asks its one child for one worker, then tells it to shut down."""

    def __init__(self, address: AddressConfig) -> None:
        self.address = address
        self.shutdown_requested = False
        self.child_gone = asyncio.Event()
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

        if isinstance(message, WorkerManagerDisconnectNotification):
            self.child_gone.set()
            return

        assert isinstance(message, WorkerManagerHeartbeat)
        await self._binder.send(source, WorkerManagerHeartbeatEcho(), detached=True)
        if self.shutdown_requested:
            await self._binder.send(source, WorkerManagerShutdown(), detached=True)
            return

        request = WorkerManagerCommand.DesiredTaskConcurrencyRequest(taskConcurrency=1, capabilities=[])
        await self._binder.send(
            source, WorkerManagerCommand(setDesiredTaskConcurrencyRequests=[request]), detached=True
        )


class TestNestedDrain(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

    async def test_a_child_manager_drains_its_running_task_before_it_exits(self) -> None:
        """WorkerManagerShutdown lets the running task finish once, then the child reports its fleet gone."""
        combo = SchedulerClusterCombo(n_workers=0)
        parent = _FakeParent(AddressConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"))
        parent_task = asyncio.ensure_future(parent.run())
        child_process = multiprocessing.get_context("spawn").Process(
            target=NativeWorkerManager(
                NativeWorkerManagerConfig(
                    worker_manager_config=WorkerManagerConfig(
                        scheduler_address=AddressConfig.from_string(combo.get_address()),
                        worker_manager_id="drain-test",
                        max_task_concurrency=1,
                        parent_address=parent.address,
                        unit_id=UNIT_ID,
                    ),
                    worker_config=WorkerConfig(heartbeat_interval_seconds=1),
                )
            ).run
        )
        start_log_fd, start_log_path = tempfile.mkstemp(prefix="scaler_drain_", suffix=".log")
        os.close(start_log_fd)
        loop = asyncio.get_running_loop()
        try:
            await asyncio.sleep(0.5)  # let the fake parent bind before the child dials it
            child_process.start()

            with Client(address=combo.get_address()) as client:
                future = client.submit(_record_start_then_sleep, start_log_path, TASK_SECONDS)
                while os.path.getsize(start_log_path) == 0:
                    await asyncio.sleep(0.1)

                parent.shutdown_requested = True
                result = await loop.run_in_executor(None, future.result, TIMEOUT_SECONDS)

            self.assertEqual(result, TASK_SECONDS)
            with open(start_log_path) as start_log:
                self.assertEqual(len(start_log.readlines()), 1, "the drained task must run once, not restart")

            await asyncio.wait_for(parent.child_gone.wait(), TIMEOUT_SECONDS)
            await loop.run_in_executor(None, child_process.join, TIMEOUT_SECONDS)
            self.assertEqual(child_process.exitcode, 0)
        finally:
            if child_process.is_alive():
                child_process.kill()
                child_process.join()
            parent_task.cancel()
            parent.destroy()
            combo.shutdown()
            os.unlink(start_log_path)
