import time
from typing import Dict, Optional

import psutil

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.message import Resource, WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.python.status import ProcessorStatus
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager, ProcessorManager, TaskManager, TimeoutManager
from scaler.worker.agent.processor_holder import ProcessorHolder


class VanillaHeartbeatManager(Looper, HeartbeatManager):
    def __init__(
        self, object_storage_address: Optional[ObjectStorageConfig], capabilities: Dict[str, int], task_queue_size: int
    ):
        self._agent_process = psutil.Process()
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._worker_task_manager: Optional[TaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None
        self._processor_manager: Optional[ProcessorManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

        self._object_storage_address: Optional[ObjectStorageConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager: TaskManager,
        timeout_manager: TimeoutManager,
        processor_manager: ProcessorManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager
        self._processor_manager = processor_manager

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        if self._start_timestamp_ns == 0:
            # not handling echo if we didn't send out heartbeat
            return

        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        if self._object_storage_address is None:
            address_message = heartbeat.object_storage_address()
            self._object_storage_address = ObjectStorageConfig(address_message.host, address_message.port)
            await self._connector_storage.connect(self._object_storage_address.host, self._object_storage_address.port)

    async def routine(self):
        processors = self._processor_manager.processors()

        if self._start_timestamp_ns != 0:
            # already sent heartbeat, expecting heartbeat echo, so not sending
            return

        for processor_holder in processors:
            status = processor_holder.process().status()
            if status in {psutil.STATUS_ZOMBIE, psutil.STATUS_DEAD}:
                await self._processor_manager.on_failing_processor(processor_holder.processor_id(), status)

        processors = self._processor_manager.processors()  # refreshes for removed dead and zombie processors
        num_suspended_processors = self._processor_manager.num_suspended_processors()

        # TODO: add task queue size to WorkerHeartbeat
        await self._connector_external.send(
            WorkerHeartbeat.new_msg(
                Resource.new_msg(int(self._agent_process.cpu_percent() * 10), self._agent_process.memory_info().rss),
                psutil.virtual_memory().available,
                self._task_queue_size,
                self._worker_task_manager.get_queued_size() - num_suspended_processors,
                self._latency_us,
                self._processor_manager.can_accept_task(),
                [self.__get_processor_status_from_holder(processor) for processor in processors],
                self._capabilities,
            )
        )
        self._start_timestamp_ns = time.time_ns()

    def get_object_storage_address(self) -> Optional[ObjectStorageConfig]:
        return self._object_storage_address

    @staticmethod
    def __get_processor_status_from_holder(processor: ProcessorHolder) -> ProcessorStatus:
        process = processor.process()

        try:
            resource = Resource.new_msg(int(process.cpu_percent() * 10), process.memory_info().rss)
        except psutil.ZombieProcess:
            # Assumes dead processes do not use any resources
            resource = Resource.new_msg(0, 0)

        return ProcessorStatus.new_msg(
            processor.pid(), processor.initialized(), processor.task() is not None, processor.suspended(), resource
        )
