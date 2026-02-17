"""
AWS Batch Heartbeat Manager.

Sends periodic heartbeats to the scheduler with worker status.
"""

import time
from typing import Dict, Optional

import psutil

from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.message import WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.python.status import ProcessorStatus, Resource
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager, TimeoutManager


class AWSBatchHeartbeatManager(Looper, HeartbeatManager):
    """
    Heartbeat manager for AWS Batch worker adapter.
    """

    def __init__(
        self,
        object_storage_address: Optional[ObjectStorageAddressConfig],
        capabilities: Dict[str, int],
        task_queue_size: int,
    ):
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size
        self._agent_process = psutil.Process()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._task_manager = None  # AWSHPCTaskManager
        self._timeout_manager: Optional[TimeoutManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0
        self._object_storage_address: Optional[ObjectStorageAddressConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager,  # AWSHPCTaskManager
        timeout_manager: TimeoutManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._task_manager = worker_task_manager
        self._timeout_manager = timeout_manager

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        if self._start_timestamp_ns == 0:
            return
        
        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        if self._object_storage_address is None:
            address_message = heartbeat.object_storage_address()
            self._object_storage_address = ObjectStorageAddressConfig(address_message.host, address_message.port)
            await self._connector_storage.connect(
                self._object_storage_address.host,
                self._object_storage_address.port
            )

    def get_object_storage_address(self) -> Optional[ObjectStorageAddressConfig]:
        return self._object_storage_address

    async def routine(self):
        if self._start_timestamp_ns != 0:
            return

        self._start_timestamp_ns = time.time_ns()

        try:
            agent_cpu = int(self._agent_process.cpu_percent())
            agent_rss = self._agent_process.memory_info().rss
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            agent_cpu = 0
            agent_rss = 0

        rss_free = psutil.virtual_memory().available
        
        # Get pending job count from task manager
        queued_tasks = self._task_manager.get_queued_size() if self._task_manager else 0
        processing_tasks = len(self._task_manager._processing_task_ids) if self._task_manager else 0
        has_task = processing_tasks > 0
        task_lock = not self._task_manager.can_accept_task() if self._task_manager else False

        # Create agent resource (cpu percentage, rss memory)
        agent_resource = Resource.new_msg(cpu=agent_cpu, rss=agent_rss)
        
        # Create processor status for the adapter (simulated as single processor)
        # pid=0 since AWS Batch jobs run remotely, initialized=True means ready
        processor_resource = Resource.new_msg(cpu=0, rss=0)
        processor_status = ProcessorStatus.new_msg(
            pid=0,
            initialized=True,
            has_task=has_task,
            suspended=False,
            resource=processor_resource,
        )

        heartbeat = WorkerHeartbeat.new_msg(
            agent=agent_resource,
            rss_free=rss_free,
            queue_size=self._task_queue_size,
            queued_tasks=queued_tasks + processing_tasks,
            latency_us=self._latency_us,
            task_lock=task_lock,
            processors=[processor_status],
            capabilities=self._capabilities,
        )

        await self._connector_external.send(heartbeat)
