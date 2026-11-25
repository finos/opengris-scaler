import asyncio
import time
import unittest

import psutil

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.defaults import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_PROCESSOR_KILL_DELAY_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
)
from scaler.config.types.address import AddressConfig
from scaler.io.mixins import AsyncBinder
from scaler.io.network_backends import get_network_backend_from_env
from scaler.protocol.capnp import BaseMessage, ProcessorInitialized
from scaler.utility.logging.utility import setup_logger
from scaler.worker.agent.processor_holder import ProcessorHolder
from tests.utility.utility import logging_test_name

START_TIMEOUT_SECONDS = 60


class TestProcessorHolderKill(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.object_storage = ObjectStorageServerProcess(
            bind_address=AddressConfig.from_string("tcp://127.0.0.1:0"),
            identity="ObjectStorageServer",
            logging_paths=("/dev/stdout",),
            logging_config_file=None,
            logging_level="INFO",
        )
        self.object_storage.start()
        self.addCleanup(self.object_storage.join)
        self.addCleanup(self.object_storage.kill)
        self.object_storage_address = self.object_storage.wait_until_ready()

    async def test_an_idle_processor_exits_through_its_own_teardown(self) -> None:
        """kill() on an idle processor stops it cleanly, without falling back to killing the process."""
        initialized = asyncio.Event()

        async def on_receive(source: bytes, message: BaseMessage) -> None:
            if isinstance(message, ProcessorInitialized):
                initialized.set()

        backend = get_network_backend_from_env(io_threads=1)
        agent = backend.create_async_binder(identity=b"agent", callback=on_receive)
        await agent.bind(AddressConfig.from_string("tcp://127.0.0.1:0"))
        assert agent.address is not None
        agent_routine = asyncio.ensure_future(self.__run_forever(agent))
        self.addCleanup(agent.destroy)
        self.addCleanup(agent_routine.cancel)

        holder = ProcessorHolder(
            event_loop="builtin",
            agent_address=agent.address,
            scheduler_address=agent.address,
            object_storage_address=self.object_storage_address,
            preload=None,
            garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
            trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
            hard_suspend=False,
            logging_paths=("/dev/stdout",),
            logging_level="INFO",
        )
        self.addCleanup(self.__kill_if_alive, holder.process())
        await asyncio.wait_for(initialized.wait(), START_TIMEOUT_SECONDS)

        start = time.monotonic()
        holder.kill()

        self.assertEqual(holder.exitcode(), 0, "a killed process exits with a signal or TerminateProcess code")
        self.assertLess(time.monotonic() - start, DEFAULT_PROCESSOR_KILL_DELAY_SECONDS, "it fell back to a kill")

    @staticmethod
    def __kill_if_alive(process: psutil.Process) -> None:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass

    @staticmethod
    async def __run_forever(agent: AsyncBinder) -> None:
        while True:
            await agent.routine()
