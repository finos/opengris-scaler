import asyncio
import logging
import pickle
from concurrent.futures import Future
from typing import Dict, Optional, Set, cast

import cloudpickle
from bidict import bidict

from scaler import Serializer
from scaler.io.mixins import AsyncConnector, AsyncObjectStorageConnector
from scaler.protocol.python.common import ObjectMetadata, ObjectStorageAddress, TaskCancelConfirmType, TaskResultType
from scaler.protocol.python.message import ObjectInstruction, Task, TaskCancel, TaskCancelConfirm, TaskResult
from scaler.utility.identifiers import ObjectID, TaskID
from scaler.utility.metadata.task_flags import retrieve_task_flags_from_task
from scaler.utility.mixins import Looper
from scaler.utility.queues.async_sorted_priority_queue import AsyncSortedPriorityQueue
from scaler.utility.serialization import serialize_failure
from scaler.worker.agent.mixins import HeartbeatManager, TaskManager


class AWSBatchTaskManager(Looper, TaskManager):
    """
    AWS Batch Task Manager that handles task execution via AWS Batch jobs.
    Similar to Symphony task manager but submits tasks to AWS Batch instead of Symphony.
    """

    def __init__(
        self,
        base_concurrency: int,
        job_queue: str,
        job_definition: str,
        aws_region: str,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
    ):
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a positive integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._job_queue = job_queue
        self._job_definition = job_definition
        self._aws_region = aws_region
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key

        # Task execution control
        self._executor_semaphore = asyncio.Semaphore(value=self._base_concurrency)

        # Task tracking
        self._task_id_to_task: Dict[TaskID, Task] = dict()
        self._task_id_to_future: bidict[TaskID, asyncio.Future] = bidict()
        self._task_id_to_batch_job_id: Dict[TaskID, str] = dict()

        # Serializer cache
        self._serializers: Dict[bytes, Serializer] = dict()

        # Task queues and state tracking
        self._queued_task_id_queue = AsyncSortedPriorityQueue()
        self._queued_task_ids: Set[bytes] = set()
        self._acquiring_task_ids: Set[TaskID] = set()  # tasks contesting the semaphore
        self._processing_task_ids: Set[TaskID] = set()
        self._canceled_task_ids: Set[TaskID] = set()

        # Object storage
        self._object_storage_address: Optional[ObjectStorageAddress] = None

        # Connectors
        self._connector_external: Optional[AsyncConnector] = None
        self._connector_storage: Optional[AsyncObjectStorageConnector] = None

        # AWS Batch client
        self._batch_client = None
        self._initialize_batch_client()

    def _initialize_batch_client(self):
        """Initialize AWS Batch client for job submission."""
        # For testing: skip actual AWS client initialization
        self._batch_client = None
        print(f"AWS Batch task manager initialized (mock mode)")

    def register(
        self,
        connector_external: AsyncConnector,
        connector_storage: AsyncObjectStorageConnector,
        heartbeat_manager: HeartbeatManager,
    ):
        """Register required components."""
        self._connector_external = connector_external
        self._connector_storage = connector_storage
        self._heartbeat_manager = heartbeat_manager

    async def routine(self):
        """Task manager routine - AWS Batch has two main loops like Symphony."""
        pass

    async def on_object_instruction(self, instruction: ObjectInstruction):
        """Handle object lifecycle instructions."""
        if instruction.instruction_type == ObjectInstruction.ObjectInstructionType.Delete:
            for object_id in instruction.object_metadata.object_ids:
                self._serializers.pop(object_id, None)  # we only cache serializers
            return

        logging.error(f"worker received unknown object instruction type {instruction=}")

    async def on_task_new(self, task: Task):
        """
        Handle new task submission.
        Just execute the task directly without queuing or priority logic.
        """
        print(f"*** AWS BATCH TASK MANAGER: on_task_new() invoked for task {task.task_id.hex()[:8]} ***")
        # Pickle task object to file for debugging
        task_file = "/tmp/debug_task.pkl"
        with open(task_file, 'wb') as f:
            pickle.dump(task, f)
        print(f"DEBUG: Task pickled to {task_file}")
        
        # Store task and execute immediately
        self._task_id_to_task[task.task_id] = task
        self._processing_task_ids.add(task.task_id)
        self._task_id_to_future[task.task_id] = await self.__execute_task_local(task)

    async def on_cancel_task(self, task_cancel: TaskCancel):
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
            self._queued_task_ids.remove(task_cancel.task_id)
            self._queued_task_id_queue.remove(task_cancel.task_id)
            self._task_id_to_task.pop(task_cancel.task_id)

        # Handle processing task cancellation
        if task_processing:
            future = self._task_id_to_future[task_cancel.task_id]
            future.cancel()

            # Cancel AWS Batch job if it exists
            if task_cancel.task_id in self._task_id_to_batch_job_id:
                batch_job_id = self._task_id_to_batch_job_id[task_cancel.task_id]
                print(f"Mock canceling AWS Batch job {batch_job_id}")

            self._processing_task_ids.remove(task_cancel.task_id)
            self._canceled_task_ids.add(task_cancel.task_id)

        result = TaskCancelConfirm.new_msg(
            task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled
        )
        await self._connector_external.send(result)

    async def on_task_result(self, result: TaskResult):
        """Handle task result processing."""
        if result.task_id in self._queued_task_ids:
            self._queued_task_ids.remove(result.task_id)
            self._queued_task_id_queue.remove(result.task_id)

        self._processing_task_ids.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)

        # Clean up batch job tracking
        self._task_id_to_batch_job_id.pop(result.task_id, None)

        await self._connector_external.send(result)

    def get_queued_size(self):
        """Get number of queued tasks."""
        return self._queued_task_id_queue.qsize()

    def can_accept_task(self):
        """Check if more tasks can be accepted."""
        return not self._executor_semaphore.locked()

    async def resolve_tasks(self):
        """Resolve completed task futures and handle results."""
        if not self._task_id_to_future:
            await asyncio.sleep(0)
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            task = self._task_id_to_task[task_id]

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

    async def process_task(self):
        """Process next queued task."""
        await self._executor_semaphore.acquire()

        _, task_id = await self._queued_task_id_queue.get()
        task = self._task_id_to_task[task_id]

        self._acquiring_task_ids.add(task_id)
        self._processing_task_ids.add(task_id)
        self._task_id_to_future[task.task_id] = await self.__execute_task_local(task)

    async def __execute_task_local(self, task: Task) -> asyncio.Future:
        """
        Execute a task via AWS Batch job submission.
        
        Args:
            task: The task to execute
            
        Returns:
            asyncio.Future: Future that will resolve when the batch job completes
        """
        serializer_id = ObjectID.generate_serializer_object_id(task.source)

        if serializer_id not in self._serializers:
            serializer_bytes = await self._connector_storage.get_object(serializer_id)
            serializer = cloudpickle.loads(serializer_bytes)
            self._serializers[serializer_id] = serializer
        else:
            serializer = self._serializers[serializer_id]

        # Fetches the function object and the argument objects concurrently
        get_tasks = [
            self._connector_storage.get_object(object_id)
            for object_id in [task.func_object_id, *(cast(ObjectID, arg) for arg in task.function_args)]
        ]

        function_bytes, *arg_bytes = await asyncio.gather(*get_tasks)

        function = serializer.deserialize(function_bytes)
        arg_objects = [serializer.deserialize(object_bytes) for object_bytes in arg_bytes]
        
        # Pickle function and args for debugging
        with open('/tmp/debug_function.pkl', 'wb') as f:
            pickle.dump(function, f)
        with open('/tmp/debug_args.pkl', 'wb') as f:
            pickle.dump(arg_objects, f)
        print(f"DEBUG: Function and args pickled to /tmp/debug_function.pkl and /tmp/debug_args.pkl")

        # TODO: Submit to AWS Batch instead of executing locally
        # For now, execute locally for testing
        future: Future = Future()
        future.set_running_or_notify_cancel()
        
        try:
            result = function(*arg_objects)
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)

        return asyncio.wrap_future(future)

    async def _submit_batch_job(self, task: Task) -> asyncio.Future:
        """
        Submit a task to AWS Batch and return a future that resolves when complete.
        
        Args:
            task: The task to submit to AWS Batch
            
        Returns:
            asyncio.Future: Future that will resolve when the batch job completes
        """
        # TODO: Implement AWS Batch job submission
        # 1. Prepare task data for batch job
        # 2. Submit job using batch_client.submit_job()
        # 3. Store job ID for tracking
        # 4. Start monitoring job status
        # 5. Return future that resolves when job completes
        raise NotImplementedError("AWS Batch job submission is not yet implemented")

    async def _cancel_batch_job(self, job_id: str):
        """Cancel an AWS Batch job."""
        # TODO: Implement AWS Batch job cancellation
        # Use batch_client.cancel_job() or terminate_job()
        raise NotImplementedError("AWS Batch job cancellation is not yet implemented")

    def _create_task_job_parameters(self, task: Task, function_data: bytes, args_data: list) -> dict:
        """
        Create AWS Batch job parameters for a specific task.
        
        Args:
            task: The Scaler task
            function_data: Serialized function
            args_data: List of serialized arguments
            
        Returns:
            dict: AWS Batch job parameters
        """
        # TODO: Create job parameters including:
        # - Unique job name based on task ID
        # - Container overrides with task-specific environment
        # - Task data passed as environment variables or S3 objects
        # - Proper resource requirements
        raise NotImplementedError("Task-specific job parameter creation is not yet implemented")

    async def _monitor_batch_job(self, job_id: str, future: asyncio.Future):
        """
        Monitor an AWS Batch job and resolve the future when complete.
        
        Args:
            job_id: AWS Batch job ID to monitor
            future: Future to resolve when job completes
        """
        # TODO: Implement job monitoring
        # 1. Poll job status periodically
        # 2. When job completes, fetch results
        # 3. Resolve future with results or exception
        raise NotImplementedError("AWS Batch job monitoring is not yet implemented")

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        """Get task priority from task metadata."""
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority