"""
AWS HPC Task Manager.

Handles task queuing, priority, semaphore, and execution via AWS Batch.
Supports array jobs for batching multiple tasks into a single Batch submission.
Follows the same pattern as SymphonyTaskManager for consistency.
"""

import asyncio
import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Set, Tuple, cast

import cloudpickle
from bidict import bidict

from scaler import Serializer
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata, TaskCancelConfirmType, TaskResultType
from scaler.protocol.python.message import ObjectInstruction, Task, TaskCancel, TaskCancelConfirm, TaskResult
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_priority_queue import AsyncPriorityQueue
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager, TaskManager

# Array job batching constants
ARRAY_JOB_BATCH_WINDOW_SECONDS: float = 0.5
ARRAY_JOB_MIN_BATCH_SIZE: int = 2
ARRAY_JOB_MAX_BATCH_SIZE: int = 10000  # AWS Batch limit


class AWSHPCTaskManager(Looper, TaskManager):
    """
    AWS HPC Task Manager that handles task execution via AWS Batch jobs.
    Follows the same pattern as SymphonyTaskManager for consistency.
    """

    def __init__(
        self,
        base_concurrency: int,
        job_queue: str,
        job_definition: str,
        aws_region: str,
        s3_bucket: str,
        s3_prefix: str = "scaler-tasks",
        job_timeout_seconds: int = 3600,
    ) -> None:
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a positive integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._job_queue = job_queue
        self._job_definition = job_definition
        self._aws_region = aws_region
        self._s3_bucket = s3_bucket
        self._s3_prefix = s3_prefix
        self._job_timeout_seconds = job_timeout_seconds

        # Task execution control
        self._executor_semaphore = asyncio.Semaphore(value=self._base_concurrency)

        # Task tracking
        self._task_id_to_task: Dict[TaskID, Task] = dict()
        self._task_id_to_future: bidict[TaskID, asyncio.Future] = bidict()
        self._task_id_to_batch_job_id: Dict[TaskID, str] = dict()

        # Serializer cache
        self._serializers: Dict[bytes, Serializer] = dict()

        # Task queues and state tracking
        self._queued_task_id_queue = AsyncPriorityQueue()
        self._queued_task_ids: Set[bytes] = set()
        self._acquiring_task_ids: Set[TaskID] = set()  # tasks contesting the semaphore
        self._processing_task_ids: Set[TaskID] = set()
        self._canceled_task_ids: Set[TaskID] = set()

        # Connectors
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None
        self._heartbeat_manager: Optional[HeartbeatManager] = None

        # AWS clients (initialized lazily)
        self._batch_client: Any = None
        self._s3_client: Any = None

        # Thread pool for non-blocking boto3 calls
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="aws-hpc")

        # Array job batching: accumulate tasks before submitting as one array job
        self._batch_pending: List[Tuple[Task, Any, List[Any], Future]] = []
        self._batch_window_start: float = 0.0

    def _initialize_aws_clients(self) -> None:
        """Initialize AWS Batch and S3 clients."""
        import boto3

        session = boto3.Session(region_name=self._aws_region)
        self._batch_client = session.client("batch")
        self._s3_client = session.client("s3")
        logging.info(f"AWS HPC task manager initialized: region={self._aws_region}, queue={self._job_queue}")

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        heartbeat_manager: HeartbeatManager,
    ) -> None:
        """Register required components."""
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._heartbeat_manager = heartbeat_manager
        self._initialize_aws_clients()

    async def routine(self) -> None:
        """Task manager routine - flush any pending batch that has waited long enough."""
        if not self._batch_pending:
            return

        elapsed = time.monotonic() - self._batch_window_start
        if elapsed >= ARRAY_JOB_BATCH_WINDOW_SECONDS:
            await self._flush_pending_batch()

    async def on_object_instruction(self, instruction: ObjectInstruction) -> None:
        """Handle object lifecycle instructions."""
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            for object_id in instruction.object_metadata.object_ids:
                self._serializers.pop(object_id, None)  # we only cache serializers
            return

        logging.error(f"worker received unknown object instruction type {instruction=}")

    async def on_task_new(self, task: Task) -> None:
        """
        Handle new task submission.
        Uses priority queue like Symphony for task ordering.
        """
        task_priority = self.__get_task_priority(task)

        # if semaphore is locked, check if task is higher priority than all acquired tasks
        # if so, bypass acquiring and execute the task immediately
        if self._executor_semaphore.locked():
            for acquired_task_id in self._acquiring_task_ids:
                acquired_task = self._task_id_to_task[acquired_task_id]
                acquired_task_priority = self.__get_task_priority(acquired_task)
                if task_priority <= acquired_task_priority:
                    break
            else:
                self._task_id_to_task[task.task_id] = task
                self._processing_task_ids.add(task.task_id)
                self._task_id_to_future[task.task_id] = await self.__execute_task(task)
                return

        self._task_id_to_task[task.task_id] = task
        self._queued_task_id_queue.put_nowait((-task_priority, task.task_id))
        self._queued_task_ids.add(task.task_id)

    async def on_cancel_task(self, task_cancel: TaskCancel) -> None:
        """Handle task cancellation requests."""
        task_queued = task_cancel.task_id in self._queued_task_ids
        task_processing = task_cancel.task_id in self._processing_task_ids

        if not task_queued and not task_processing:
            await self._connector_external.send(
                TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.CancelNotFound
                )
            )
            return

        if task_processing and not task_cancel.flags.force:
            await self._connector_external.send(
                TaskCancelConfirm.new_msg(
                    task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.CancelFailed
                )
            )
            return

        # Handle queued task cancellation
        if task_queued:
            self._queued_task_ids.discard(task_cancel.task_id)
            self._queued_task_id_queue.remove(task_cancel.task_id)
            self._task_id_to_task.pop(task_cancel.task_id, None)

        # Handle processing task cancellation
        if task_processing:
            future = self._task_id_to_future.get(task_cancel.task_id)
            if future is not None:
                future.cancel()

            # Cancel AWS Batch job if it exists
            if task_cancel.task_id in self._task_id_to_batch_job_id:
                batch_job_id = self._task_id_to_batch_job_id[task_cancel.task_id]
                await self._cancel_batch_job(batch_job_id)

            self._processing_task_ids.discard(task_cancel.task_id)
            self._task_id_to_task.pop(task_cancel.task_id, None)
            self._canceled_task_ids.add(task_cancel.task_id)

        result = TaskCancelConfirm.new_msg(
            task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled
        )
        await self._connector_external.send(result)

    async def on_task_result(self, result: TaskResult) -> None:
        """Handle task result processing."""
        if result.task_id in self._queued_task_ids:
            self._queued_task_ids.discard(result.task_id)
            self._queued_task_id_queue.remove(result.task_id)

        self._processing_task_ids.discard(result.task_id)
        self._task_id_to_task.pop(result.task_id, None)

        # Clean up batch job tracking
        self._task_id_to_batch_job_id.pop(result.task_id, None)

        await self._connector_external.send(result)

    def get_queued_size(self) -> int:
        """Get number of queued tasks."""
        return self._queued_task_id_queue.qsize()

    def can_accept_task(self) -> bool:
        """Check if more tasks can be accepted."""
        return not self._executor_semaphore.locked()

    async def resolve_tasks(self) -> None:
        """Resolve completed task futures and handle results."""
        if not self._task_id_to_future:
            await asyncio.sleep(0.1)  # Small sleep to avoid CPU spin when idle
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            task = self._task_id_to_task.get(task_id)

            if task is None:
                logging.warning(f"Cannot find task in worker queue: task_id={task_id.hex()}")
                continue

            if task_id in self._processing_task_ids:
                self._processing_task_ids.remove(task_id)

                if future.exception() is None:
                    # Success case
                    serializer_id = ObjectID.generate_serializer_object_id(task.source)
                    serializer = self._serializers[serializer_id]
                    result_bytes = serializer.serialize(future.result())
                    result_type = TaskResultType.Success
                else:
                    # Failure case
                    result_bytes = serialize_failure(cast(Exception, future.exception()))
                    result_type = TaskResultType.Failed

                # Store result in object storage
                result_object_id = ObjectID.generate_object_id(task.source)
                await self._connector_storage.set_object(result_object_id, result_bytes)

                # Notify about object creation
                await self._connector_external.send(
                    ObjectInstruction.new_msg(
                        ObjectInstruction.ObjectInstructionType.Create,
                        task.source,
                        ObjectMetadata.new_msg(
                            object_ids=(result_object_id,),
                            object_types=(ObjectMetadata.ObjectContentType.Object,),
                            object_names=(f"<res {result_object_id.hex()[:6]}>".encode(),),
                        ),
                    )
                )

                # Send task result
                await self._connector_external.send(
                    TaskResult.new_msg(task_id, result_type, metadata=b"", results=[bytes(result_object_id)])
                )

            elif task_id in self._canceled_task_ids:
                self._canceled_task_ids.remove(task_id)
            else:
                raise ValueError(f"task_id {task_id.hex()} not found in processing or canceled tasks")

            # Release semaphore
            if task_id in self._acquiring_task_ids:
                self._acquiring_task_ids.remove(task_id)
                self._executor_semaphore.release()

            # Clean up
            self._task_id_to_task.pop(task_id)
            self._task_id_to_batch_job_id.pop(task_id, None)

    async def process_task(self) -> None:
        """Process next queued task."""
        await self._executor_semaphore.acquire()

        _, task_id = await self._queued_task_id_queue.get()
        task = self._task_id_to_task[task_id]

        self._acquiring_task_ids.add(task_id)
        self._processing_task_ids.add(task_id)
        self._task_id_to_future[task.task_id] = await self.__execute_task(task)

    async def __execute_task(self, task: Task) -> asyncio.Future:
        """
        Prepare a task for AWS Batch execution and add it to the pending batch.
        The batch is flushed by routine() after the batch window expires,
        or immediately if the batch reaches max size.
        """
        serializer_id = ObjectID.generate_serializer_object_id(task.source)

        if serializer_id not in self._serializers:
            serializer_bytes = await self._connector_storage.get_object(serializer_id)
            serializer = cloudpickle.loads(serializer_bytes)
            self._serializers[serializer_id] = serializer
        else:
            serializer = self._serializers[serializer_id]

        # Fetch function and arguments concurrently
        get_tasks = [
            self._connector_storage.get_object(object_id)
            for object_id in [task.func_object_id, *(cast(ObjectID, arg) for arg in task.function_args)]
        ]

        function_bytes, *arg_bytes = await asyncio.gather(*get_tasks)

        function = serializer.deserialize(function_bytes)
        arg_objects = [serializer.deserialize(object_bytes) for object_bytes in arg_bytes]

        # Create future for this task
        future: Future = Future()
        future.set_running_or_notify_cancel()

        # Add to batch pending queue
        self._batch_pending.append((task, function, arg_objects, future))
        if len(self._batch_pending) == 1:
            self._batch_window_start = time.monotonic()

        # If batch is full, flush immediately
        if len(self._batch_pending) >= ARRAY_JOB_MAX_BATCH_SIZE:
            await self._flush_pending_batch()

        return asyncio.wrap_future(future)

    async def _flush_pending_batch(self) -> None:
        """Flush all pending tasks as a single array job or individual job."""
        if not self._batch_pending:
            return

        pending = self._batch_pending[:]
        self._batch_pending.clear()

        batch = [(t, f, a) for t, f, a, _ in pending]
        futures_map: Dict[TaskID, Future] = {t.task_id: fut for t, _, _, fut in pending}

        await self._flush_batch(batch, futures_map)

    async def _flush_batch(self, batch: List[Tuple[Task, Any, List[Any]]], futures_map: Dict[TaskID, Future]) -> None:
        """Submit a batch of tasks as either an array job or a single job."""
        try:
            if len(batch) >= ARRAY_JOB_MIN_BATCH_SIZE:
                await self._submit_array_job(batch, futures_map)
            else:
                single_task, single_func, single_args = batch[0]
                single_future = futures_map[single_task.task_id]
                batch_job_id = await self._submit_single_batch_job(single_task, single_func, single_args)
                self._task_id_to_batch_job_id[single_task.task_id] = batch_job_id
                logging.info(f"Task {single_task.task_id.hex()[:8]} submitted as Batch job {batch_job_id}")
                asyncio.create_task(self._monitor_batch_job(batch_job_id, single_future, single_task.task_id))
        except Exception as e:
            logging.exception(f"Failed to submit batch of {len(batch)} tasks: {e}")
            for f in futures_map.values():
                if not f.done():
                    f.set_exception(e)

    async def _run_in_executor(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a blocking function in the thread pool to avoid starving the event loop."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, lambda: func(*args, **kwargs))

    async def _submit_array_job(
        self, batch: List[Tuple[Task, Any, List[Any]]], futures_map: Dict[TaskID, Future]
    ) -> None:
        """Submit multiple tasks as a single AWS Batch array job."""
        import gzip
        import re
        import uuid

        from botocore.exceptions import ClientError

        array_id = uuid.uuid4().hex[:12]
        array_size = len(batch)

        # Upload all payloads to S3 with index-based keys
        s3_prefix_array = f"{self._s3_prefix}/array/{array_id}"
        index_to_task_id: Dict[int, TaskID] = {}

        for index, (task, function, arguments) in enumerate(batch):
            task_data = {
                "task_id": task.task_id.hex(),
                "source": task.source.hex(),
                "function": function,
                "arguments": arguments,
            }
            payload = cloudpickle.dumps(task_data)
            if len(payload) > 4 * 1024:
                payload = gzip.compress(payload)

            s3_key = f"{s3_prefix_array}/{index}.pkl"
            await self._run_in_executor(self._s3_client.put_object, Bucket=self._s3_bucket, Key=s3_key, Body=payload)
            index_to_task_id[index] = task.task_id

        # Get function name for job naming
        _, first_func, _ = batch[0]
        func_name = getattr(first_func, "__name__", "unknown")
        safe_func_name = re.sub(r"[^a-zA-Z0-9_-]", "_", func_name)[:50]
        job_name = f"{safe_func_name}-array-{array_id}"

        submit_kwargs: Dict[str, Any] = dict(
            jobName=job_name,
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            arrayProperties={"size": array_size},
            parameters={
                "task_id": "array",
                "payload": "none",
                "compressed": "0",
                "s3_bucket": self._s3_bucket,
                "s3_prefix": self._s3_prefix,
                "s3_key": f"{s3_prefix_array}/ARRAY_INDEX.pkl",
            },
            timeout={"attemptDurationSeconds": self._job_timeout_seconds},
        )

        try:
            response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
        except ClientError as e:
            if "ExpiredToken" in str(e) or "expired" in str(e).lower():
                logging.warning("AWS credentials expired, refreshing...")
                self._initialize_aws_clients()
                response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
            else:
                raise

        parent_job_id = response["jobId"]
        logging.info(
            f"Submitted array job {parent_job_id} with {array_size} tasks "
            f"(s3://{self._s3_bucket}/{s3_prefix_array}/)"
        )

        # Track all child jobs
        for index, task_id in index_to_task_id.items():
            child_job_id = f"{parent_job_id}:{index}"
            self._task_id_to_batch_job_id[task_id] = child_job_id

        # Start monitoring the array job
        asyncio.create_task(self._monitor_array_job(parent_job_id, index_to_task_id, futures_map, s3_prefix_array))

    async def _monitor_array_job(
        self,
        parent_job_id: str,
        index_to_task_id: Dict[int, TaskID],
        futures_map: Dict[TaskID, Future],
        s3_prefix_array: str,
    ) -> None:
        """Monitor an AWS Batch array job and resolve individual task futures."""
        import gzip

        poll_interval = 3.0
        resolved: Set[int] = set()
        total = len(index_to_task_id)

        while len(resolved) < total:
            await asyncio.sleep(poll_interval)

            try:
                unresolved_indices = [i for i in index_to_task_id if i not in resolved]
                for batch_start in range(0, len(unresolved_indices), 100):
                    batch_indices = unresolved_indices[batch_start : batch_start + 100]
                    child_job_ids = [f"{parent_job_id}:{i}" for i in batch_indices]

                    response = await self._run_in_executor(self._batch_client.describe_jobs, jobs=child_job_ids)
                    for job in response.get("jobs", []):
                        child_id = job["jobId"]
                        index = int(child_id.split(":")[-1])
                        status = job["status"]
                        task_id = index_to_task_id[index]
                        future = futures_map.get(task_id)
                        logging.debug(f"Array child {child_id}: status={status}, index={index}")

                        if future is None or future.done():
                            resolved.add(index)
                            continue

                        if status == "SUCCEEDED":
                            # The runner stores results using {AWS_BATCH_JOB_ID}:{AWS_BATCH_JOB_ARRAY_INDEX}
                            # which equals {parent_job_id}:{index}
                            result_key = f"{self._s3_prefix}/results/{parent_job_id}:{index}.pkl"
                            try:
                                # Retry with delay - result may not be written yet when status changes
                                s3_resp = None
                                for attempt in range(5):
                                    for key_candidate in [
                                        result_key,
                                        result_key + ".gz",
                                        f"{self._s3_prefix}/results/{child_id}.pkl",
                                        f"{self._s3_prefix}/results/{child_id}.pkl.gz",
                                    ]:
                                        try:
                                            s3_resp = await self._run_in_executor(
                                                self._s3_client.get_object, Bucket=self._s3_bucket, Key=key_candidate
                                            )
                                            result_key = key_candidate
                                            break
                                        except self._s3_client.exceptions.NoSuchKey:
                                            continue
                                    if s3_resp is not None:
                                        break
                                    await asyncio.sleep(3)

                                if s3_resp is None:
                                    raise RuntimeError(f"Result not found for index {index} after 5 retries")
                                result_bytes = s3_resp["Body"].read()
                                if len(result_bytes) >= 2 and result_bytes[0:2] == b"\x1f\x8b":
                                    result_bytes = gzip.decompress(result_bytes)
                                result = cloudpickle.loads(result_bytes)
                                future.set_result(result)
                                await self._run_in_executor(
                                    self._s3_client.delete_object, Bucket=self._s3_bucket, Key=result_key
                                )
                            except Exception as e:
                                future.set_exception(RuntimeError(f"Failed to fetch result for index {index}: {e}"))
                            resolved.add(index)

                        elif status == "FAILED":
                            reason = job.get("statusReason", "Unknown failure")
                            future.set_exception(RuntimeError(f"Array job child {index} failed: {reason}"))
                            resolved.add(index)

            except Exception as e:
                logging.exception(f"Error monitoring array job {parent_job_id}: {e}")

        # Cleanup S3 array payloads
        try:
            for index in index_to_task_id:
                await self._run_in_executor(
                    self._s3_client.delete_object, Bucket=self._s3_bucket, Key=f"{s3_prefix_array}/{index}.pkl"
                )
        except Exception as e:
            logging.warning(f"Failed to cleanup array payloads: {e}")

    async def _submit_single_batch_job(self, task: Task, function: Any, arguments: List[Any]) -> str:
        """Submit a single task as an individual AWS Batch job."""
        import base64
        import gzip
        import re

        from botocore.exceptions import ClientError

        task_id_hex = task.task_id.hex()
        func_name = getattr(function, "__name__", "unknown")

        task_data = {"task_id": task_id_hex, "source": task.source.hex(), "function": function, "arguments": arguments}

        payload = cloudpickle.dumps(task_data)
        payload_size = len(payload)

        compressed = False
        if payload_size > 4 * 1024:
            payload = gzip.compress(payload)
            compressed = True

        safe_func_name = re.sub(r"[^a-zA-Z0-9_-]", "_", func_name)[:50]
        job_name = f"{safe_func_name}-{task_id_hex[:12]}"

        max_inline_size = 28 * 1024

        if len(payload) <= max_inline_size:
            encoded_payload = base64.b64encode(payload).decode("ascii")
            s3_key = "none"
        else:
            s3_key = f"{self._s3_prefix}/inputs/{task_id_hex}.pkl"
            if compressed:
                s3_key += ".gz"
            await self._run_in_executor(self._s3_client.put_object, Bucket=self._s3_bucket, Key=s3_key, Body=payload)
            encoded_payload = ""

        submit_kwargs: Dict[str, Any] = dict(
            jobName=job_name,
            jobQueue=self._job_queue,
            jobDefinition=self._job_definition,
            parameters={
                "task_id": task_id_hex,
                "payload": encoded_payload,
                "compressed": "1" if compressed else "0",
                "s3_bucket": self._s3_bucket,
                "s3_prefix": self._s3_prefix,
                "s3_key": s3_key,
            },
            timeout={"attemptDurationSeconds": self._job_timeout_seconds},
        )

        try:
            response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
        except ClientError as e:
            if "ExpiredToken" in str(e) or "expired" in str(e).lower():
                logging.warning("AWS credentials expired, refreshing...")
                self._initialize_aws_clients()
                response = await self._run_in_executor(self._batch_client.submit_job, **submit_kwargs)
            else:
                raise

        return response["jobId"]

    async def _monitor_batch_job(self, job_id: str, future: Future, task_id: TaskID) -> None:
        """Monitor AWS Batch job and resolve future when complete."""
        import gzip

        poll_interval = 2.0  # seconds

        while True:
            await asyncio.sleep(poll_interval)

            try:
                response = await self._run_in_executor(self._batch_client.describe_jobs, jobs=[job_id])
                if not response.get("jobs"):
                    continue

                job = response["jobs"][0]
                status = job["status"]

                if status == "SUCCEEDED":
                    # Fetch result from S3
                    result_key = f"{self._s3_prefix}/results/{job_id}.pkl"
                    try:
                        try:
                            response = await self._run_in_executor(
                                self._s3_client.get_object, Bucket=self._s3_bucket, Key=result_key
                            )
                        except self._s3_client.exceptions.NoSuchKey:
                            result_key += ".gz"
                            response = await self._run_in_executor(
                                self._s3_client.get_object, Bucket=self._s3_bucket, Key=result_key
                            )
                        result_bytes = response["Body"].read()

                        # Check if compressed
                        if len(result_bytes) >= 2 and result_bytes[0:2] == b"\x1f\x8b":
                            result_bytes = gzip.decompress(result_bytes)

                        result = cloudpickle.loads(result_bytes)
                        future.set_result(result)

                        # Cleanup S3
                        await self._run_in_executor(
                            self._s3_client.delete_object, Bucket=self._s3_bucket, Key=result_key
                        )
                    except Exception as e:
                        future.set_exception(RuntimeError(f"Failed to fetch result: {e}"))
                    return

                elif status == "FAILED":
                    reason = job.get("statusReason", "Unknown failure")
                    log_output = await self._fetch_job_logs(job_id)
                    error_msg = f"Batch job failed: {reason}"
                    if log_output:
                        error_msg += f"\n\n{log_output}"
                    future.set_exception(RuntimeError(error_msg))
                    return

                elif status in ("SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"):
                    continue
                else:
                    logging.warning(f"Unknown job status: {status}")

            except Exception as e:
                logging.exception(f"Error monitoring job {job_id}: {e}")

    async def _cancel_batch_job(self, job_id: str) -> None:
        """Cancel an AWS Batch job."""
        try:
            await self._run_in_executor(self._batch_client.terminate_job, jobId=job_id, reason="Canceled by Scaler")
            logging.info(f"Canceled Batch job {job_id}")
        except Exception as e:
            logging.warning(f"Failed to cancel Batch job {job_id}: {e}")

    async def _fetch_job_logs(self, job_id: str) -> str:
        """Fetch CloudWatch logs for a failed job."""
        try:
            import boto3

            logs_client = boto3.client("logs", region_name=self._aws_region)

            log_group = "/aws/batch/job"

            job_response = self._batch_client.describe_jobs(jobs=[job_id])
            if not job_response.get("jobs"):
                return "(Job not found)"

            job = job_response["jobs"][0]
            container = job.get("container", {})
            log_stream = container.get("logStreamName", "")

            debug_info = [
                f"Job status: {job.get('status')}",
                f"Status reason: {job.get('statusReason', 'N/A')}",
                f"Container exit code: {container.get('exitCode', 'N/A')}",
                f"Container reason: {container.get('reason', 'N/A')}",
            ]

            if not log_stream:
                return "Job debug info:\n" + "\n".join(debug_info) + "\n\n(Could not determine log stream name)"

            await asyncio.sleep(2)

            try:
                response = logs_client.get_log_events(
                    logGroupName=log_group, logStreamName=log_stream, limit=100, startFromHead=True
                )

                events = response.get("events", [])
                if not events:
                    return "Job debug info:\n" + "\n".join(debug_info) + "\n\n(No log events found)"

                log_lines = [event.get("message", "") for event in events]
                return "Job debug info:\n" + "\n".join(debug_info) + "\n\nContainer logs:\n" + "\n".join(log_lines)

            except logs_client.exceptions.ResourceNotFoundException:
                return "Job debug info:\n" + "\n".join(debug_info) + f"\n\n(Log stream not found: {log_stream})"

        except Exception as e:
            logging.warning(f"Failed to fetch logs for job {job_id}: {e}")
            return f"(Failed to fetch logs: {e})"

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        """Get task priority from task metadata."""
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
