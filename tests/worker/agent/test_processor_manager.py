import asyncio
import unittest
from unittest.mock import Mock

from scaler.config.types.address import AddressConfig, SocketType
from scaler.utility.identifiers import ProcessorID, WorkerID
from scaler.worker.agent.processor_manager import VanillaProcessorManager


def _make_manager() -> VanillaProcessorManager:
    return VanillaProcessorManager(
        identity=WorkerID.generate_worker_id("w"),
        event_loop="builtin",
        address_internal=AddressConfig(SocketType.tcp, "127.0.0.1", 1),
        scheduler_address=AddressConfig(SocketType.tcp, "127.0.0.1", 2),
        preload=None,
        garbage_collect_interval_seconds=1,
        trim_memory_threshold_bytes=0,
        hard_processor_suspend=False,
        logging_paths=(),
        logging_level="INFO",
    )


class TestProcessorInitializedWait(unittest.IsolatedAsyncioTestCase):
    """The actor manager waits for the processor to report ProcessorInitialized instead of
    polling; this verifies the wait primitive that replaced the poll loop."""

    async def test_wait_blocks_until_processor_reports_initialized(self) -> None:
        manager = _make_manager()

        # before any processor reports in, the wait must block
        self.assertFalse(manager.current_processor_is_initialized())
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(manager.wait_until_current_processor_initialized(), timeout=0.1)

        # simulate a processor reporting ProcessorInitialized: initialize() acquires the
        # can-accept-task lock, and on_processor_initialized releases it and sets the event
        await manager._can_accept_task_lock.acquire()  # noqa: SLF001
        holder = Mock()
        holder.initialized.return_value = False
        manager._current_holder = holder  # noqa: SLF001
        await manager.on_processor_initialized(ProcessorID.generate_processor_id(), Mock())

        # the waiter now wakes immediately
        await asyncio.wait_for(manager.wait_until_current_processor_initialized(), timeout=1)


if __name__ == "__main__":
    unittest.main()
