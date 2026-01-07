import time
from typing import Dict, Optional

import psutil

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.message import WorkerHeartbeat, WorkerHeartbeatEcho
from scaler.protocol.python.status import Resource
from scaler.utility.mixins import Looper
from scaler.worker.agent.mixins import HeartbeatManager, TimeoutManager
from scaler.worker_adapter.aws_batch.task_manager import AWSBatchTaskManager


class AWSBatchHeartbeatManager(Looper, HeartbeatManager):
    """
    AWS Batch Heartbeat Manager that manages worker heartbeats for AWS Batch workers.
    Similar to Symphony heartbeat manager but adapted for AWS Batch environment.
    """

    def __init__(
        self, 
        object_storage_address: Optional[ObjectStorageConfig], 
        capabilities: Dict[str, int], 
        task_queue_size: int
    ):
        self._capabilities = capabilities
        self._task_queue_size = task_queue_size

        # Process monitoring
        self._agent_process = psutil.Process()

        # Component references
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._worker_task_manager: Optional[AWSBatchTaskManager] = None
        self._timeout_manager: Optional[TimeoutManager] = None

        # Heartbeat timing
        self._start_timestamp_ns = 0
        self._latency_us = 0

        # Object storage configuration
        self._object_storage_address: Optional[ObjectStorageConfig] = object_storage_address

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        worker_task_manager: AWSBatchTaskManager,
        timeout_manager: TimeoutManager,
    ):
        """Register required components."""
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._worker_task_manager = worker_task_manager
        self._timeout_manager = timeout_manager

    async def on_heartbeat_echo(self, heartbeat: WorkerHeartbeatEcho):
        """
        Handle heartbeat echo from scheduler.
        
        Args:
            heartbeat: Heartbeat echo message from scheduler
        """
        if self._start_timestamp_ns == 0:
            # Not handling echo if we didn't send out heartbeat
            return

        # Calculate round-trip latency
        self._latency_us = int(((time.time_ns() - self._start_timestamp_ns) / 2) // 1_000)
        self._start_timestamp_ns = 0
        self._timeout_manager.update_last_seen_time()

        # Handle object storage address discovery
        if self._object_storage_address is None:
            address_message = heartbeat.object_storage_address()
            self._object_storage_address = ObjectStorageConfig(address_message.host, address_message.port)
            await self._connector_storage.connect(self._object_storage_address.host, self._object_storage_address.port)

    def get_object_storage_address(self) -> Optional[ObjectStorageConfig]:
        """Get the current object storage address."""
        return self._object_storage_address

    async def routine(self):
        """
        Send periodic heartbeat to scheduler.
        This is the main heartbeat routine that runs periodically.
        """
        if self._start_timestamp_ns != 0:
            # Don't send another heartbeat if we're still waiting for echo
            return

        # TODO: Enhance heartbeat with AWS Batch specific metrics
        # Could include:
        # - Number of active batch jobs
        # - Batch job queue status
        # - AWS resource utilization
        # - Batch-specific error rates

        await self._connector_external.send(
            WorkerHeartbeat.new_msg(
                agent=Resource.new_msg(
                    int(self._agent_process.cpu_percent() * 10), 
                    self._agent_process.memory_info().rss
                ),
                rss_free=psutil.virtual_memory().available,
                queue_size=self._task_queue_size,
                queued_tasks=self._worker_task_manager.get_queued_size(),
                latency_us=self._latency_us,
                task_lock=not self._worker_task_manager.can_accept_task(),
                processors=self._get_batch_processor_status(),
                capabilities=self._capabilities,
            )
        )
        self._start_timestamp_ns = time.time_ns()

    def _get_batch_processor_status(self) -> list:
        """
        Get processor status information for AWS Batch workers.
        
        Returns:
            list: List of processor status objects (empty for now)
        """
        # TODO: Implement AWS Batch specific processor status
        # This could include:
        # - Status of active batch jobs
        # - Resource utilization per job
        # - Job execution metrics
        # For now, return empty list as AWS Batch jobs are external processes
        return []

    def _get_batch_metrics(self) -> Dict[str, any]:
        """
        Get AWS Batch specific metrics for heartbeat.
        
        Returns:
            Dict[str, any]: Batch-specific metrics
        """
        # TODO: Implement AWS Batch metrics collection
        # Could include:
        # - Active job count
        # - Job success/failure rates
        # - Queue wait times
        # - Resource utilization
        # - Cost metrics
        raise NotImplementedError("AWS Batch metrics collection is not yet implemented")

    def _check_batch_job_health(self) -> bool:
        """
        Check health of managed AWS Batch jobs.
        
        Returns:
            bool: True if all jobs are healthy
        """
        # TODO: Implement batch job health checking
        # 1. Query status of all managed jobs
        # 2. Check for failed or stuck jobs
        # 3. Return overall health status
        raise NotImplementedError("AWS Batch job health checking is not yet implemented")