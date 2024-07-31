import asyncio
import logging
import os
import signal
from typing import Optional, Tuple

import psutil

from scaler.io.config import DEFAULT_PROCESSOR_KILL_DELAY_SECONDS
from scaler.protocol.python.message import Task
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.agent.processor.processor import Processor


class ProcessorHolder:
    def __init__(
        self,
        event_loop: str,
        address: ZMQConfig,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        logging_paths: Tuple[str, ...],
        logging_level: str,
    ):
        self._processor_id: Optional[bytes] = None
        self._task: Optional[Task] = None
        self._initialized = asyncio.Event()
        self._suspended = False

        self._processor = Processor(
            event_loop=event_loop,
            address=address,
            garbage_collect_interval_seconds=garbage_collect_interval_seconds,
            trim_memory_threshold_bytes=trim_memory_threshold_bytes,
            logging_paths=logging_paths,
            logging_level=logging_level,
        )
        self._processor.start()
        self._process = psutil.Process(self._processor.pid)

    def pid(self) -> int:
        assert self._processor.pid is not None
        return self._processor.pid

    def process(self) -> psutil.Process:
        return self._process

    def processor_id(self) -> bytes:
        assert self._processor_id is not None
        return self._processor_id

    def initialized(self) -> bool:
        return self._initialized.is_set()

    def wait_initialized(self):
        return self._initialized.wait()

    def set_initialized(self, processor_id: bytes):
        self._processor_id = processor_id
        self._initialized.set()

    def task(self) -> Optional[Task]:
        return self._task

    def set_task(self, task: Optional[Task]):
        self._task = task

    def suspended(self) -> bool:
        return self._suspended

    def suspend(self):
        assert self._processor is not None
        assert self._task is not None
        assert self._suspended is False

        os.kill(self.pid(), signal.SIGSTOP)  # type: ignore
        self._suspended = True

    def resume(self):
        assert self._task is not None
        assert self._suspended is True

        os.kill(self.pid(), signal.SIGCONT)  # type: ignore
        self._suspended = False

    def kill(self):
        self.__send_signal(signal.SIGTERM)
        self._processor.join(DEFAULT_PROCESSOR_KILL_DELAY_SECONDS)

        if self._processor.exitcode is None:
            # TODO: some processors fail to interrupt because of a blocking 0mq call. Ideally we should interrupt
            # these blocking calls instead of sending a SIGKILL signal.

            logging.warn(f"Processor[{self.pid()}] does not terminate in time, send SIGKILL.")
            self.__send_signal(signal.SIGKILL)
            self._processor.join()

        self.set_task(None)

    def __send_signal(self, sig: int):
        os.kill(self.pid(), sig)
