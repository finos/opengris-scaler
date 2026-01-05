"""
AWS Batch Heartbeat Manager.

Sends periodic heartbeats to the scheduler with worker status.
"""

import time
from typing import Dict, Optional

import psutil

from scaler.config.types.object_storage_server import ObjectStorageConfig
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
        object_storage_address: Optional[ObjectStorageConfig],
        capabilities: Dict[str, int],
        task_queue_size: int,
    ):
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size
        self._agent_process = psutil.Process()

        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._adapter = None  # AWSBatchWorkerAdapter
        self._timeout_manager: Optional[TimeoutManager] = None

        self._start_timestamp_ns = 0
        self._latency_us = 0
        self._object_storage_address: Optional[ObjectStorageConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager,  # AWSBatchWorkerAdapter
        timeout_manager: TimeoutManager,
    ):
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._adapter = worker_task_manager
        self._timeout_manager = timeout_manager

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        if self._start_timestamp_ns == 0:
            return
        
        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        if self._object_storage_address is None:
            address_message = heartbeat.object_storage_address()
            self._object_storage_address = ObjectStorageConfig(address_message.host, address_message.port)
            await self._connector_storage.connect(
                self._object_storage_address.host,
                self._object_storage_address.port
            )

    def get_object_storage_address(self) -> Optional[ObjectStorageConfig]:
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
        
        # Get pending job count from adapter
        pending_jobs = len(self._adapter._task_id_to_batch_job_id) if self._adapter else 0
        has_task = pending_jobs > 0
        task_lock = self._adapter._semaphore.locked() if self._adapter else False

        heartbeat = WorkerHeartbeat.new_msg(
            resource=Resource.new_msg(
                agent_cpu=agent_cpu,
                agent_rss=agent_rss,
                worker_cpu=0,
                worker_rss=0,
                rss_free=rss_free,
            ),
            queued_tasks=pending_jobs,
            latency_us=self._latency_us,
            processor_status=ProcessorStatus.new_msg(
                initialized=True,
                has_task=has_task,
                task_lock=task_lock,
            ),
            capabilities=self._capabilities,
        )

        await self._connector_external.send(heartbeat)
