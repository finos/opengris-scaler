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
from scaler.worker_adapter.aws_hpc.callback import BatchJobCallback

# AWS Batch limits (https://docs.aws.amazon.com/batch/latest/userguide/service_limits.html)
MAX_PARAMS_SIZE_BYTES = 30 * 1024  # 30KiB for job parameters
MAX_INLINE_PAYLOAD_BYTES = 28 * 1024  # Leave room for other params
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
        Fetches function and arguments from object storage and embeds them in payload.
        Compresses payload if it exceeds threshold.
        Uses S3 for payloads that exceed AWS Batch limits.
        """
        task_id_hex = task.task_id.hex()
        
        # Fetch the actual function from object storage
        func_bytes = await self._connector_storage.get_object(task.func_object_id)
        func = cloudpickle.loads(func_bytes)
        
        # Get function name for job naming
        func_name = getattr(func, '__name__', 'unknown')
        
        # Fetch arguments from object storage
        arguments = []
        for arg in task.function_args:
            if isinstance(arg, TaskID):
                # TaskID means result of another task - this shouldn't happen in simple cases
                raise ValueError(f"Task dependencies (TaskID args) not yet supported in AWS Batch adapter")
            else:  # ObjectID
                arg_bytes = await self._connector_storage.get_object(arg)
                arg_value = cloudpickle.loads(arg_bytes)
                arguments.append(arg_value)
        
        # Create payload with embedded function and arguments
        task_data = {
            "task_id": task_id_hex,
            "source": task.source.hex(),
            "function": func,
            "arguments": arguments,
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
                return await self._submit_with_inline_payload(task_id_hex, func_name, compressed, compressed=True)
            else:
                # Store in S3
                return await self._submit_with_s3_payload(task_id_hex, func_name, compressed, compressed=True)
        elif payload_size <= MAX_INLINE_PAYLOAD_BYTES:
            # Use uncompressed inline payload
            return await self._submit_with_inline_payload(task_id_hex, func_name, payload, compressed=False)
        else:
            # Store in S3 (uncompressed but too large for inline)
            return await self._submit_with_s3_payload(task_id_hex, func_name, payload, compressed=False)

    async def _submit_with_inline_payload(
        self, 
        task_id_hex: str,
        func_name: str,
        payload: bytes, 
        compressed: bool
    ) -> str:
        """Submit job with payload in job parameters."""
        import base64
        import re
        from botocore.exceptions import ClientError
        
        encoded_payload = base64.b64encode(payload).decode("ascii")
        
        # Create descriptive job name: func_name-task_id (sanitized for AWS Batch)
        # AWS Batch job names: up to 128 chars, alphanumeric, hyphens, underscores
        safe_func_name = re.sub(r'[^a-zA-Z0-9_-]', '_', func_name)[:50]
        job_name = f"{safe_func_name}-{task_id_hex[:12]}"
        
        try:
            response = self._batch_client.submit_job(
                jobName=job_name,
                jobQueue=self._job_queue,
                jobDefinition=self._job_definition,
                parameters={
                    "task_id": task_id_hex,
                    "payload": encoded_payload,
                    "compressed": "1" if compressed else "0",
                    "s3_bucket": self._s3_bucket,
                    "s3_prefix": self._s3_prefix,
                    "s3_key": "none",
                },
            )
        except ClientError as e:
            if "ExpiredToken" in str(e) or "expired" in str(e).lower():
                logging.warning("AWS credentials expired, refreshing...")
                self._refresh_aws_clients()
                response = self._batch_client.submit_job(
                    jobName=job_name,
                    jobQueue=self._job_queue,
                    jobDefinition=self._job_definition,
                    parameters={
                        "task_id": task_id_hex,
                        "payload": encoded_payload,
                        "compressed": "1" if compressed else "0",
                        "s3_bucket": self._s3_bucket,
                        "s3_prefix": self._s3_prefix,
                        "s3_key": "none",
                    },
                )
            else:
                raise
        
        return response["jobId"]

    async def _submit_with_s3_payload(
        self, 
        task_id_hex: str,
        func_name: str,
        payload: bytes, 
        compressed: bool
    ) -> str:
        """Submit job with payload stored in S3."""
        import re
        from botocore.exceptions import ClientError
        
        s3_key = f"{self._s3_prefix}/inputs/{task_id_hex}.pkl"
        if compressed:
            s3_key += ".gz"
        
        # Create descriptive job name
        safe_func_name = re.sub(r'[^a-zA-Z0-9_-]', '_', func_name)[:50]
        job_name = f"{safe_func_name}-{task_id_hex[:12]}"
        
        try:
            # Upload to S3
            self._s3_client.put_object(
                Bucket=self._s3_bucket,
                Key=s3_key,
                Body=payload
            )
            
            response = self._batch_client.submit_job(
                jobName=job_name,
                jobQueue=self._job_queue,
                jobDefinition=self._job_definition,
                parameters={
                    "task_id": task_id_hex,
                    "payload": "",  # Empty, use S3
                    "compressed": "1" if compressed else "0",
                    "s3_bucket": self._s3_bucket,
                    "s3_prefix": self._s3_prefix,
                    "s3_key": s3_key,
                },
            )
        except ClientError as e:
            if "ExpiredToken" in str(e) or "expired" in str(e).lower():
                logging.warning("AWS credentials expired, refreshing...")
                self._refresh_aws_clients()
                self._s3_client.put_object(
                    Bucket=self._s3_bucket,
                    Key=s3_key,
                    Body=payload
                )
                response = self._batch_client.submit_job(
                    jobName=job_name,
                    jobQueue=self._job_queue,
                    jobDefinition=self._job_definition,
                    parameters={
                        "task_id": task_id_hex,
                        "payload": "",
                        "compressed": "1" if compressed else "0",
                        "s3_bucket": self._s3_bucket,
                        "s3_prefix": self._s3_prefix,
                        "s3_key": s3_key,
                    },
                )
            else:
                raise
        
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
            # ObjectID requires 32 bytes: 16 bytes owner hash + 16 bytes unique tag
            result_object_id = ObjectID(uuid.uuid4().bytes + uuid.uuid4().bytes)
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
        logging.error(f"Fetching CloudWatch logs for job {job_id}...")
        
        # Try to fetch CloudWatch logs for debugging
        log_output = await self._fetch_job_logs(job_id)
        
        error_msg = f"Batch job failed: {reason}"
        if log_output:
            error_msg += f"\n\n{log_output}"
            # Print logs prominently
            logging.error("=" * 60)
            logging.error(f"BATCH JOB LOGS ({job_id}):")
            logging.error("=" * 60)
            logging.error(log_output)
            logging.error("=" * 60)
        
        await self._send_task_failed(task_id, b"", RuntimeError(error_msg))

    async def _fetch_job_logs(self, job_id: str) -> str:
        """Fetch CloudWatch logs for a failed job."""
        try:
            import boto3
            import time
            logs_client = boto3.client("logs", region_name=self._aws_region)
            
            # AWS Batch logs go to /aws/batch/job log group
            log_group = "/aws/batch/job"
            
            # Get job details to find log stream
            job_response = self._batch_client.describe_jobs(jobs=[job_id])
            if not job_response.get("jobs"):
                return "(Job not found)"
            
            job = job_response["jobs"][0]
            container = job.get("container", {})
            
            # Get log stream name - AWS Batch provides this directly
            log_stream = container.get("logStreamName", "")
            
            # Debug info
            debug_info = []
            debug_info.append(f"Job status: {job.get('status')}")
            debug_info.append(f"Status reason: {job.get('statusReason', 'N/A')}")
            debug_info.append(f"Container exit code: {container.get('exitCode', 'N/A')}")
            debug_info.append(f"Container reason: {container.get('reason', 'N/A')}")
            
            if not log_stream:
                # Try to construct from task ARN
                task_arn = container.get("taskArn", "")
                if task_arn:
                    task_id_part = task_arn.split("/")[-1]
                    job_def_name = job.get("jobDefinition", "").split("/")[-1].split(":")[0]
                    log_stream = f"{job_def_name}/default/{task_id_part}"
                    debug_info.append(f"Constructed log stream: {log_stream}")
                else:
                    debug_info.append("No taskArn available")
            else:
                debug_info.append(f"Log stream: {log_stream}")
            
            if not log_stream:
                return "Job debug info:\n" + "\n".join(debug_info) + "\n\n(Could not determine log stream name)"
            
            # Wait a moment for logs to be available
            await asyncio.sleep(2)
            
            # Fetch log events
            try:
                response = logs_client.get_log_events(
                    logGroupName=log_group,
                    logStreamName=log_stream,
                    limit=100,
                    startFromHead=True  # Get from beginning to see startup errors
                )
                
                events = response.get("events", [])
                if not events:
                    return "Job debug info:\n" + "\n".join(debug_info) + "\n\n(No log events found - container may have crashed before logging)"
                
                # Format log output
                log_lines = [event.get("message", "") for event in events]
                return "Job debug info:\n" + "\n".join(debug_info) + "\n\nContainer logs:\n" + "\n".join(log_lines)
                
            except logs_client.exceptions.ResourceNotFoundException:
                return "Job debug info:\n" + "\n".join(debug_info) + f"\n\n(Log stream not found: {log_stream})"
                
        except Exception as e:
            logging.warning(f"Failed to fetch logs for job {job_id}: {e}")
            return f"(Failed to fetch logs: {e})"

    async def _send_task_failed(self, task_id: TaskID, source: bytes, exception: Exception):
        """Send task failure to scheduler."""
        from scaler.utility.serialization import serialize_failure
        
        error_bytes = serialize_failure(exception)
        # ObjectID requires 32 bytes: 16 bytes owner hash + 16 bytes unique tag
        result_object_id = ObjectID(uuid.uuid4().bytes + uuid.uuid4().bytes)
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
