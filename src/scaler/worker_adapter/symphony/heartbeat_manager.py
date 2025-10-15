import time
from typing import Dict, Optional

import psutil

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.message import WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.python.status import Resource
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager, TimeoutManager
from scaler.worker_adapter.symphony.task_manager import SymphonyTaskManager


class SymphonyHeartbeatManager(Looper, HeartbeatManager):
    def __init__(
        self, object_storage_address: Optional[ObjectStorageConfig], capabilities: Dict[str, int], task_queue_size: int
    ):
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size

        self._agent_process = psutil.Process()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._worker_task_manager: Optional[SymphonyTaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0

        self._object_storage_address: Optional[ObjectStorageConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager: SymphonyTaskManager,
        timeout_manager: TimeoutManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager

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

    def get_object_storage_address(self) -> Optional[ObjectStorageConfig]:
        return self._object_storage_address

    async def routine(self):
        if self._start_timestamp_ns != 0:
            return

        await self._connector_external.send(
            WorkerHeartbeat.new_msg(
                Resource.new_msg(int(self._agent_process.cpu_percent() * 10), self._agent_process.memory_info().rss),
                psutil.virtual_memory().available,
                self._task_queue_size,
                self._worker_task_manager.get_queued_size(),
                self._latency_us,
                self._worker_task_manager.can_accept_task(),
                [],
                self._capabilities,
            )
        )
        self._start_timestamp_ns = time.time_ns()
