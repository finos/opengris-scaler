"""
AWS Batch Worker Adapter.

Receives tasks from the Scaler scheduler streaming mechanism and submits
them directly as AWS Batch jobs. Large payloads are compressed before
submission to stay within AWS Batch limits.

AWS Batch Limits:
- Container overrides environment: 8KB total
- Job parameters: 20KB total
- For larger payloads, data is stored in S3 and referenced by key
"""

import asyncio
import gzip
import logging
import uuid
from typing import Dict, Optional

import cloudpickle

from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.common import TaskResultType
from scaler.protocol.python.message import (
    ObjectInstruction,
    Task,
    TaskCancel,
    TaskResult,
)
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.worker_adapter.aws_batch.callback import BatchJobCallback

# AWS Batch limits
MAX_ENV_SIZE_BYTES = 8 * 1024  # 8KB for environment variables
MAX_INLINE_PAYLOAD_BYTES = 6 * 1024  # Leave room for other env vars
COMPRESSION_THRESHOLD_BYTES = 4 * 1024  # Compress if larger than 4KB


class AWSBatchWorkerAdapter:
    """
    AWS Batch Worker Adapter that processes scheduler stream tasks
    and submits them as AWS Batch jobs.
    
    Flow:
        Scheduler Stream → Task → Compress if needed → AWS Batch Job
        AWS Batch Job → Result → Scheduler Stream
    """

    def __init__(
        self,
        job_queue: str,
        job_definition: str,
        aws_region: str,
        s3_bucket: str,
        s3_prefix: str = "scaler-tasks",
        max_concurrent_jobs: int = 100,
        poll_interval_seconds: float = 1.0,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        self._job_queue = job_queue
        self._job_definition = job_definition
        self._aws_region = aws_region
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._max_concurrent_jobs = max_concurrent_jobs
        self._poll_interval = poll_interval_seconds
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

        # Job tracking
        self._callback = BatchJobCallback()
        self._task_id_to_batch_job_id: Dict[TaskID, str] = {}
        self._semaphore = asyncio.Semaphore(max_concurrent_jobs)

        # Connectors (set via register)
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        # AWS clients
        self._batch_client = None
        self._s3_client = None

    def _initialize_aws_clients(self):
        """Initialize AWS Batch and S3 clients."""
        import boto3
        
        session_kwargs = {"region_name": self._aws_region}
        if self._aws_access_key_id and self._aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self._aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self._aws_secret_access_key
        
        session = boto3.Session(**session_kwargs)
        self._batch_client = session.client("batch")
        self._s3_client = session.client("s3")
        logging.info(f"AWS Batch adapter initialized: region={self._aws_region}, queue={self._job_queue}")

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
    ):
        """Register connectors for scheduler communication."""
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._initialize_aws_clients()

    async def on_task(self, task: Task):
        """
        Handle incoming task from scheduler stream.
        Submits task as AWS Batch job.
        """
        await self._semaphore.acquire()
        
        try:
            batch_job_id = await self._submit_batch_job(task)
            self._task_id_to_batch_job_id[task.task_id] = batch_job_id
            logging.info(f"Task {task.task_id.hex()[:8]} submitted as Batch job {batch_job_id}")
        except Exception as e:
            logging.exception(f"Failed to submit task {task.task_id.hex()[:8]}: {e}")
            self._semaphore.release()
            await self._send_task_failed(task.task_id, task.source, e)

    async def on_task_cancel(self, task_cancel: TaskCancel):
        """Handle task cancellation from scheduler stream."""
        task_id = task_cancel.task_id
        batch_job_id = self._task_id_to_batch_job_id.pop(task_id, None)
        
        if batch_job_id:
            try:
                self._batch_client.terminate_job(
                    jobId=batch_job_id,
                    reason="Canceled by Scaler"
                )
                logging.info(f"Canceled Batch job {batch_job_id} for task {task_id.hex()[:8]}")
            except Exception as e:
                logging.warning(f"Failed to cancel Batch job {batch_job_id}: {e}")
        
        self._callback.cancel_task(task_id.hex())

    async def _submit_batch_job(self, task: Task) -> str:
        """
        Submit task as AWS Batch job.
        Compresses payload if it exceeds threshold.
        Uses S3 for payloads that exceed AWS Batch limits.
        """
        task_id_hex = task.task_id.hex()
        
        # Serialize task data
        task_data = {
            "task_id": task_id_hex,
            "source": task.source.hex(),
            "func_object_id": bytes(task.func_object_id).hex(),
            "args": [
                {"type": arg.type.name, "data": arg.data.hex() if isinstance(arg.data, bytes) else arg.data}
                for arg in task.function_args
            ],
        }
        
        payload = cloudpickle.dumps(task_data)
        payload_size = len(payload)
        
        # Determine storage method based on size
        if payload_size > COMPRESSION_THRESHOLD_BYTES:
            # Compress payload
            compressed = gzip.compress(payload)
            logging.debug(f"Compressed payload: {payload_size} -> {len(compressed)} bytes")
            
            if len(compressed) <= MAX_INLINE_PAYLOAD_BYTES:
                # Use compressed inline payload
                return await self._submit_with_inline_payload(task_id_hex, compressed, compressed=True)
            else:
                # Store in S3
                return await self._submit_with_s3_payload(task_id_hex, compressed, compressed=True)
        elif payload_size <= MAX_INLINE_PAYLOAD_BYTES:
            # Use uncompressed inline payload
            return await self._submit_with_inline_payload(task_id_hex, payload, compressed=False)
        else:
            # Store in S3 (uncompressed but too large for inline)
            return await self._submit_with_s3_payload(task_id_hex, payload, compressed=False)

    async def _submit_with_inline_payload(
        self, 
        task_id_hex: str, 
        payload: bytes, 
        compressed: bool
    ) -> str:
        """Submit job with payload in environment variable."""
        import base64
        
        encoded_payload = base64.b64encode(payload).decode("ascii")
        
        response = self._batch_client.submit_job(
            jobName=f"scaler-{task_id_hex[:12]}",
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            containerOverrides={
                "environment": [
                    {"name": "SCALER_TASK_ID", "value": task_id_hex},
                    {"name": "SCALER_PAYLOAD", "value": encoded_payload},
                    {"name": "SCALER_PAYLOAD_COMPRESSED", "value": "1" if compressed else "0"},
                    {"name": "SCALER_S3_BUCKET", "value": self._s3_bucket},
                    {"name": "SCALER_S3_PREFIX", "value": self._s3_prefix},
                ]
            }
        )
        
        return response["jobId"]

    async def _submit_with_s3_payload(
        self, 
        task_id_hex: str, 
        payload: bytes, 
        compressed: bool
    ) -> str:
        """Submit job with payload stored in S3."""
        s3_key = f"{self._s3_prefix}/inputs/{task_id_hex}.pkl"
        if compressed:
            s3_key += ".gz"
        
        # Upload to S3
        self._s3_client.put_object(
            Bucket=self._s3_bucket,
            Key=s3_key,
            Body=payload
        )
        
        response = self._batch_client.submit_job(
            jobName=f"scaler-{task_id_hex[:12]}",
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            containerOverrides={
                "environment": [
                    {"name": "SCALER_TASK_ID", "value": task_id_hex},
                    {"name": "SCALER_S3_BUCKET", "value": self._s3_bucket},
                    {"name": "SCALER_S3_KEY", "value": s3_key},
                    {"name": "SCALER_PAYLOAD_COMPRESSED", "value": "1" if compressed else "0"},
                ]
            }
        )
        
        return response["jobId"]

    async def poll_jobs(self):
        """Poll AWS Batch for job status updates."""
        pending_jobs = list(self._task_id_to_batch_job_id.items())
        if not pending_jobs:
            return

        job_ids = [job_id for _, job_id in pending_jobs]
        
        try:
            # Batch API supports up to 100 jobs per describe call
            for i in range(0, len(job_ids), 100):
                batch = job_ids[i:i+100]
                response = self._batch_client.describe_jobs(jobs=batch)
                
                for job in response.get("jobs", []):
                    await self._handle_job_status(job)
        except Exception as e:
            logging.exception(f"Error polling Batch jobs: {e}")

    async def _handle_job_status(self, job: dict):
        """Handle job status update from AWS Batch."""
        job_id = job["jobId"]
        status = job["status"]
        
        # Find task ID for this job
        task_id = None
        for tid, jid in list(self._task_id_to_batch_job_id.items()):
            if jid == job_id:
                task_id = tid
                break
        
        if task_id is None:
            return
        
        if status == "SUCCEEDED":
            await self._handle_job_succeeded(task_id, job_id)
            self._task_id_to_batch_job_id.pop(task_id, None)
            self._semaphore.release()
        elif status == "FAILED":
            reason = job.get("statusReason", "Unknown failure")
            await self._handle_job_failed(task_id, job_id, reason)
            self._task_id_to_batch_job_id.pop(task_id, None)
            self._semaphore.release()

    async def _handle_job_succeeded(self, task_id: TaskID, job_id: str):
        """Handle successful job completion."""
        try:
            # Fetch result from S3
            result_key = f"{self._s3_prefix}/results/{job_id}.pkl"
            response = self._s3_client.get_object(Bucket=self._s3_bucket, Key=result_key)
            result_bytes = response["Body"].read()
            
            # Check if compressed
            if result_key.endswith(".gz") or self._is_gzipped(result_bytes):
                result_bytes = gzip.decompress(result_bytes)
            
            # Send result to scheduler
            result_object_id = ObjectID(uuid.uuid4().bytes)
            await self._connector_storage.set_object(result_object_id, result_bytes)
            
            await self._connector_external.send(
                TaskResult.new_msg(task_id, TaskResultType.Success, results=[bytes(result_object_id)])
            )
            
            # Cleanup S3
            self._s3_client.delete_object(Bucket=self._s3_bucket, Key=result_key)
            
            logging.info(f"Task {task_id.hex()[:8]} completed successfully")
        except Exception as e:
            logging.exception(f"Error handling job success for {task_id.hex()[:8]}: {e}")
            await self._send_task_failed(task_id, b"", e)

    async def _handle_job_failed(self, task_id: TaskID, job_id: str, reason: str):
        """Handle job failure."""
        logging.error(f"Task {task_id.hex()[:8]} failed: {reason}")
        await self._send_task_failed(task_id, b"", RuntimeError(f"Batch job failed: {reason}"))

    async def _send_task_failed(self, task_id: TaskID, source: bytes, exception: Exception):
        """Send task failure to scheduler."""
        from scaler.utility.serialization import serialize_failure
        
        error_bytes = serialize_failure(exception)
        result_object_id = ObjectID(uuid.uuid4().bytes)
        await self._connector_storage.set_object(result_object_id, error_bytes)
        
        await self._connector_external.send(
            TaskResult.new_msg(task_id, TaskResultType.Failed, results=[bytes(result_object_id)])
        )

    @staticmethod
    def _is_gzipped(data: bytes) -> bool:
        """Check if data is gzip compressed."""
        return len(data) >= 2 and data[0:2] == b'\x1f\x8b'

    async def routine(self):
        """Main routine - poll for job completions."""
        await self.poll_jobs()
