import logging
import multiprocessing
import os
import signal
from typing import Optional, Tuple

import psutil

from scaler.io.config import DEFAULT_PROCESSOR_KILL_DELAY_SECONDS
from scaler.protocol.python.message import Task
from scaler.utility.identifiers import ProcessorID
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.agent.processor.processor import SUSPEND_SIGNAL, Processor


class ProcessorHolder:
    def __init__(
        self,
        event_loop: str,
        agent_address: ZMQConfig,
        storage_address: ObjectStorageConfig,
        garbage_collect_interval_seconds: int,
        trim_memory_threshold_bytes: int,
        hard_suspend: bool,
        logging_paths: Tuple[str, ...],
        logging_level: str,
    ):
        self._processor_id: Optional[ProcessorID] = None
        self._task: Optional[Task] = None
        self._suspended = False

        self._hard_suspend = hard_suspend
        if hard_suspend:
            self._resume_event = None
            self._resumed_event = None
        else:
            context = multiprocessing.get_context("spawn")
            self._resume_event = context.Event()
            self._resumed_event = context.Event()

        self._processor = Processor(
            event_loop=event_loop,
            agent_address=agent_address,
            storage_address=storage_address,
            resume_event=self._resume_event,
            resumed_event=self._resumed_event,
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

    def processor_id(self) -> ProcessorID:
        assert self._processor_id is not None
        return self._processor_id

    def initialized(self) -> bool:
        return self._processor_id is not None

    def initialize(self, processor_id: ProcessorID):
        self._processor_id = processor_id

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
        assert self.initialized()

        if self._hard_suspend:
            self.__send_signal("SIGSTOP")
        else:
            # If we do not want to hardly suspend the processor's process (e.g. to keep network links alive), we request
            # the process to wait on a synchronization event. That will stop the main thread while allowing the helper
            # threads to continue running.
            #
            # See https://github.com/Citi/scaler/issues/14

            assert self._resume_event is not None
            assert self._resumed_event is not None
            self._resume_event.clear()
            self._resumed_event.clear()

            self.__send_signal(SUSPEND_SIGNAL)

        self._suspended = True

    def resume(self):
        assert self._task is not None
        assert self._suspended is True

        if self._hard_suspend:
            self.__send_signal("SIGCONT")
        else:
            assert self._resume_event is not None
            assert self._resumed_event is not None

            self._resume_event.set()

            # Waits until the processor resumes processing. This avoids any future call to `suspend()` while the
            # processor hasn't returned from the `_resumed_event.wait()` call yet (causes a re-entrant error on Linux).
            self._resumed_event.wait()

        self._suspended = False

    def kill(self):
        self.__send_signal("SIGTERM")
        self._processor.join(DEFAULT_PROCESSOR_KILL_DELAY_SECONDS)

        if self._processor.exitcode is None:
            # TODO: some processors fail to interrupt because of a blocking 0mq call. Ideally we should interrupt
            # these blocking calls instead of sending a SIGKILL signal.

            logging.warning(f"Processor[{self.pid()}] does not terminate in time, send SIGKILL.")
            self.__send_signal("SIGKILL")
            self._processor.join()

        self.set_task(None)

    def __send_signal(self, signal_name: str):
        signal_instance = getattr(signal, signal_name, None)
        if signal_instance is None:
            raise RuntimeError(f"unsupported platform, signal not available: {signal_name}.")

        os.kill(self.pid(), signal_instance)
