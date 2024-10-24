import time
from typing import Optional

import psutil

from scaler.io.async_connector import AsyncConnector
from scaler.protocol.python.message import Resource, WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.python.status import ProcessorStatus
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager, ProcessorManager, TaskManager, TimeoutManager
from scaler.worker.agent.processor_holder import ProcessorHolder


class VanillaHeartbeatManager(Looper, HeartbeatManager):
    def __init__(self):
        self._agent_process = psutil.Process()
        self._worker_process: Optional[psutil.Process] = None

        self._connector_external: Optional[AsyncConnector] = None
        self._worker_task_manager: Optional[TaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None
        self._processor_manager: Optional[ProcessorManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

    def register(
        self,
        connector_external: AsyncConnector,
        worker_task_manager: TaskManager,
        timeout_manager: TimeoutManager,
        processor_manager: ProcessorManager,
    ):
        self._connector_external = connector_external
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager
        self._processor_manager = processor_manager

    def set_processor_pid(self, process_id: int):
        self._worker_process = psutil.Process(process_id)

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

    async def routine(self):
        if self._worker_process is None:
            return

        if self._start_timestamp_ns != 0:
            # already sent heartbeat, expecting heartbeat echo, so not sending
            return

        if self._worker_process.status() in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}:
            await self._processor_manager.on_failing_task(self._worker_process.status())

        processors = self._processor_manager.processors()
        num_suspended_processors = self._processor_manager.num_suspended_processors()

        await self._connector_external.send(
            WorkerHeartbeat.new_msg(
                Resource.new_msg(int(self._agent_process.cpu_percent() * 10), self._agent_process.memory_info().rss),
                psutil.virtual_memory().available,
                self._worker_task_manager.get_queued_size() - num_suspended_processors,
                self._latency_us,
                self._processor_manager.task_lock(),
                [self.__get_processor_status_from_holder(processor) for processor in processors],
            )
        )
        self._start_timestamp_ns = time.time_ns()

    @staticmethod
    def __get_processor_status_from_holder(processor: ProcessorHolder) -> ProcessorStatus:
        process = processor.process()
        return ProcessorStatus.new_msg(
            processor.pid(),
            processor.initialized(),
            processor.task() is not None,
            processor.suspended(),
            Resource.new_msg(int(process.cpu_percent() * 10), process.memory_info().rss),
        )
