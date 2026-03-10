"""
OCI HPC Task Manager.

Handles task queuing, priority, semaphore, and execution via OCI Container Instances.
Follows the same pattern as AWSHPCTaskManager for consistency.

OCI Service Mapping:
    - AWS Batch job       → OCI Container Instance (on-demand, per-task container)
    - Amazon S3           → OCI Object Storage (task payload and result storage)
    - AWS Batch job ID    → OCI Container Instance OCID

Container Instance Lifecycle:
    CREATING → ACTIVE → INACTIVE (finished) or FAILED
    - ACTIVE:   container is running
    - INACTIVE: container exited (check result file in Object Storage for success/failure)
    - FAILED:   instance failed to provision
"""

import asyncio
import logging
from concurrent.futures import Future
from typing import Any, Dict, List, Optional, Set, cast

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

# OCI Container Instance lifecycle states
_INSTANCE_STATE_RUNNING = {"CREATING", "ACTIVE"}
_INSTANCE_STATE_DONE = "INACTIVE"
_INSTANCE_STATE_FAILED = "FAILED"

# Object Storage key prefixes (analogous to S3 prefixes in AWSHPCTaskManager)
_KEY_INPUTS = "inputs"
_KEY_RESULTS = "results"

# Payload size threshold: store inline in environment variable if below this, else in Object Storage
_MAX_INLINE_PAYLOAD_BYTES = 28 * 1024  # 28 KB (environment variable size limit)

# Interval between container instance status polls
_POLL_INTERVAL_SECONDS = 2.0


class OCIHPCTaskManager(Looper, TaskManager):
    """
    OCI HPC Task Manager that handles task execution via OCI Container Instances.

    Each Scaler task is dispatched as an OCI Container Instance. The task payload
    (function + arguments) is either passed inline via environment variables (small
    payloads) or stored in OCI Object Storage and referenced by the container.

    Results are always written to OCI Object Storage by the container's job runner,
    then fetched and returned to the scheduler by this manager.

    Follows the same pattern as AWSHPCTaskManager for consistency.
    """

    def __init__(
        self,
        base_concurrency: int,
        compartment_id: str,
        availability_domain: str,
        subnet_id: str,
        container_image: str,
        oci_region: str,
        object_storage_namespace: str,
        object_storage_bucket: str,
        object_storage_prefix: str = "scaler-tasks",
        instance_shape: str = "CI.Standard.E4.Flex",
        instance_ocpus: float = 1.0,
        instance_memory_gb: float = 6.0,
        job_timeout_seconds: int = 3600,
        oci_config_profile: str = "DEFAULT",
    ) -> None:
        if isinstance(base_concurrency, int) and base_concurrency <= 0:
            raise ValueError(f"base_concurrency must be a positive integer, got {base_concurrency}")

        self._base_concurrency = base_concurrency
        self._compartment_id = compartment_id
        self._availability_domain = availability_domain
        self._subnet_id = subnet_id
        self._container_image = container_image
        self._oci_region = oci_region
        self._object_storage_namespace = object_storage_namespace
        self._object_storage_bucket = object_storage_bucket
        self._object_storage_prefix = object_storage_prefix
        self._instance_shape = instance_shape
        self._instance_ocpus = instance_ocpus
        self._instance_memory_gb = instance_memory_gb
        self._job_timeout_seconds = job_timeout_seconds
        self._oci_config_profile = oci_config_profile

        # Task execution control
        self._executor_semaphore = asyncio.Semaphore(value=self._base_concurrency)

        # Task tracking
        self._task_id_to_task: Dict[TaskID, Task] = {}
        self._task_id_to_future: bidict[TaskID, asyncio.Future] = bidict()
        self._task_id_to_instance_id: Dict[TaskID, str] = {}  # TaskID → Container Instance OCID

        # Serializer cache
        self._serializers: Dict[bytes, Serializer] = {}

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

        # OCI clients (initialized lazily in register())
        self._container_instances_client: Any = None
        self._object_storage_client: Any = None

    def _initialize_oci_clients(self) -> None:
        """Initialize OCI Container Instances and Object Storage clients."""
        import oci

        config = oci.config.from_file(profile_name=self._oci_config_profile)
        self._container_instances_client = oci.container_instances.ContainerInstanceClient(config)
        self._object_storage_client = oci.object_storage.ObjectStorageClient(config)

        logging.info(
            f"OCI HPC task manager initialized: region={self._oci_region}, "
            f"compartment={self._compartment_id[:20]}..., "
            f"bucket={self._object_storage_bucket}"
        )

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
        self._initialize_oci_clients()

    async def routine(self) -> None:
        """Task manager routine — two main loops (process_task and resolve_tasks) handle work."""
        pass

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
        Uses priority queue for task ordering, matching the Symphony/AWS pattern.
        """
        task_priority = self.__get_task_priority(task)

        # If semaphore is locked, check if this task has higher priority than any acquiring task.
        # If so, bypass the queue and execute immediately.
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
            self._queued_task_ids.remove(task_cancel.task_id)
            self._queued_task_id_queue.remove(task_cancel.task_id)
            self._task_id_to_task.pop(task_cancel.task_id)

        # Handle processing task cancellation
        if task_processing:
            future = self._task_id_to_future.get(task_cancel.task_id)
            if future is not None:
                future.cancel()

            # Delete the OCI Container Instance if it was submitted
            if task_cancel.task_id in self._task_id_to_instance_id:
                instance_id = self._task_id_to_instance_id[task_cancel.task_id]
                await self._delete_container_instance(instance_id)

            self._processing_task_ids.discard(task_cancel.task_id)
            self._canceled_task_ids.add(task_cancel.task_id)

        result = TaskCancelConfirm.new_msg(
            task_id=task_cancel.task_id, cancel_confirm_type=TaskCancelConfirmType.Canceled
        )
        await self._connector_external.send(result)

    async def on_task_result(self, result: TaskResult) -> None:
        """Handle task result processing."""
        if result.task_id in self._queued_task_ids:
            self._queued_task_ids.remove(result.task_id)
            self._queued_task_id_queue.remove(result.task_id)

        self._processing_task_ids.remove(result.task_id)
        self._task_id_to_task.pop(result.task_id)
        self._task_id_to_instance_id.pop(result.task_id, None)

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
            await asyncio.sleep(0.1)  # avoid busy-wait when idle
            return

        done, _ = await asyncio.wait(self._task_id_to_future.values(), return_when=asyncio.FIRST_COMPLETED)
        for future in done:
            task_id = self._task_id_to_future.inv.pop(future)
            task = self._task_id_to_task[task_id]

            if task_id in self._processing_task_ids:
                self._processing_task_ids.remove(task_id)

                if future.exception() is None:
                    serializer_id = ObjectID.generate_serializer_object_id(task.source)
                    serializer = self._serializers[serializer_id]
                    result_bytes = serializer.serialize(future.result())
                    result_type = TaskResultType.Success
                else:
                    result_bytes = serialize_failure(cast(Exception, future.exception()))
                    result_type = TaskResultType.Failed

                # Store result in Scaler object storage
                result_object_id = ObjectID.generate_object_id(task.source)
                await self._connector_storage.set_object(result_object_id, result_bytes)

                # Notify scheduler of the new result object
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

                await self._connector_external.send(
                    TaskResult.new_msg(task_id, result_type, metadata=b"", results=[bytes(result_object_id)])
                )

            elif task_id in self._canceled_task_ids:
                self._canceled_task_ids.remove(task_id)
            else:
                raise ValueError(f"task_id {task_id.hex()} not found in processing or canceled tasks")

            # Release semaphore slot
            if task_id in self._acquiring_task_ids:
                self._acquiring_task_ids.remove(task_id)
                self._executor_semaphore.release()

            # Clean up
            self._task_id_to_task.pop(task_id, None)
            self._task_id_to_instance_id.pop(task_id, None)

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
        Execute a task via an OCI Container Instance.

        Fetches function and arguments from object storage, serializes them,
        submits a Container Instance, and starts a monitoring coroutine.
        """
        serializer_id = ObjectID.generate_serializer_object_id(task.source)

        if serializer_id not in self._serializers:
            serializer_bytes = await self._connector_storage.get_object(serializer_id)
            serializer = cloudpickle.loads(serializer_bytes)
            self._serializers[serializer_id] = serializer
        else:
            serializer = self._serializers[serializer_id]

        # Fetch function and all arguments concurrently
        get_tasks = [
            self._connector_storage.get_object(object_id)
            for object_id in [task.func_object_id, *(cast(ObjectID, arg) for arg in task.function_args)]
        ]
        function_bytes, *arg_bytes = await asyncio.gather(*get_tasks)

        function = serializer.deserialize(function_bytes)
        arg_objects = [serializer.deserialize(b) for b in arg_bytes]

        future: Future = Future()
        future.set_running_or_notify_cancel()

        try:
            instance_id = await self._create_container_instance(task, function, arg_objects)
            self._task_id_to_instance_id[task.task_id] = instance_id
            logging.info(f"Task {task.task_id.hex()[:8]} submitted as Container Instance {instance_id[-20:]}")

            asyncio.create_task(self._monitor_container_instance(instance_id, future, task.task_id))
        except Exception as exc:
            logging.exception(f"Failed to submit task {task.task_id.hex()[:8]}: {exc}")
            future.set_exception(exc)

        return asyncio.wrap_future(future)

    async def _create_container_instance(self, task: Task, function: Any, arguments: List[Any]) -> str:
        """
        Create an OCI Container Instance to execute the task.

        Small payloads are passed inline as an environment variable (base64-encoded).
        Larger payloads are stored in OCI Object Storage and referenced by object key.

        Returns:
            The OCID of the created Container Instance.
        """
        import base64
        import gzip
        import re

        import oci

        task_id_hex = task.task_id.hex()
        func_name = getattr(function, "__name__", "unknown")

        task_data = {
            "task_id": task_id_hex,
            "source": task.source.hex(),
            "function": function,
            "arguments": arguments,
        }
        payload = cloudpickle.dumps(task_data)
        payload_size = len(payload)

        # Compress payloads above 4 KB
        compressed = False
        if payload_size > 4 * 1024:
            payload = gzip.compress(payload)
            compressed = True
            logging.debug(f"Compressed payload: {payload_size} -> {len(payload)} bytes")

        # Sanitize function name for OCI display name (alphanumeric, hyphens, underscores only)
        safe_func_name = re.sub(r"[^a-zA-Z0-9_-]", "_", func_name)[:50]
        display_name = f"scaler-{safe_func_name}-{task_id_hex[:12]}"

        # Choose inline or Object Storage delivery
        if len(payload) <= _MAX_INLINE_PAYLOAD_BYTES:
            encoded_payload = base64.b64encode(payload).decode("ascii")
            object_key = "none"
        else:
            suffix = ".pkl.gz" if compressed else ".pkl"
            object_key = f"{self._object_storage_prefix}/{_KEY_INPUTS}/{task_id_hex}{suffix}"
            self._object_storage_client.put_object(
                namespace_name=self._object_storage_namespace,
                bucket_name=self._object_storage_bucket,
                object_name=object_key,
                put_object_body=payload,
            )
            encoded_payload = ""

        # Environment variables passed to the container job runner
        env_vars = {
            "TASK_ID": task_id_hex,
            "OCI_NAMESPACE": self._object_storage_namespace,
            "OCI_BUCKET": self._object_storage_bucket,
            "OCI_PREFIX": self._object_storage_prefix,
            "OCI_OBJECT_KEY": object_key,
            "PAYLOAD_B64": encoded_payload,
            "COMPRESSED": "1" if compressed else "0",
        }

        env_var_models = [
            oci.container_instances.models.EnvironmentVariable(name=key, value=value)
            for key, value in env_vars.items()
        ]

        create_details = oci.container_instances.models.CreateContainerInstanceDetails(
            compartment_id=self._compartment_id,
            availability_domain=self._availability_domain,
            shape=self._instance_shape,
            shape_config=oci.container_instances.models.CreateContainerInstanceShapeConfigDetails(
                ocpus=self._instance_ocpus,
                memory_in_gbs=self._instance_memory_gb,
            ),
            containers=[
                oci.container_instances.models.CreateContainerDetails(
                    image_url=self._container_image,
                    display_name=display_name,
                    environment_variables=env_var_models,
                )
            ],
            vnics=[
                oci.container_instances.models.CreateContainerVnicDetails(
                    subnet_id=self._subnet_id,
                )
            ],
            display_name=display_name,
        )

        response = self._container_instances_client.create_container_instance(
            create_container_instance_details=create_details
        )
        return response.data.id

    async def _monitor_container_instance(self, instance_id: str, future: Future, task_id: TaskID) -> None:
        """
        Poll OCI Container Instance status until INACTIVE or FAILED, then resolve the future.

        On success (INACTIVE), fetches the result from OCI Object Storage.
        On failure (FAILED or non-zero exit), sets an exception on the future.
        """
        import gzip

        while True:
            await asyncio.sleep(_POLL_INTERVAL_SECONDS)

            try:
                response = self._container_instances_client.get_container_instance(
                    container_instance_id=instance_id
                )
                state = response.data.lifecycle_state

                if state == _INSTANCE_STATE_DONE:
                    # Container exited — fetch result from Object Storage
                    result_key = f"{self._object_storage_prefix}/{_KEY_RESULTS}/{instance_id.split('.')[-1]}.pkl"
                    try:
                        obj_response = self._object_storage_client.get_object(
                            namespace_name=self._object_storage_namespace,
                            bucket_name=self._object_storage_bucket,
                            object_name=result_key,
                        )
                        result_bytes = obj_response.data.content

                        # Decompress if gzip magic bytes are present
                        if len(result_bytes) >= 2 and result_bytes[:2] == b"\x1f\x8b":
                            result_bytes = gzip.decompress(result_bytes)

                        result = cloudpickle.loads(result_bytes)
                        future.set_result(result)

                        # Clean up result object from Object Storage
                        try:
                            self._object_storage_client.delete_object(
                                namespace_name=self._object_storage_namespace,
                                bucket_name=self._object_storage_bucket,
                                object_name=result_key,
                            )
                        except Exception as cleanup_exc:
                            logging.warning(f"Failed to clean up result object {result_key}: {cleanup_exc}")

                    except Exception as fetch_exc:
                        future.set_exception(RuntimeError(f"Failed to fetch result from Object Storage: {fetch_exc}"))

                    # Delete the container instance now that it has finished
                    await self._delete_container_instance(instance_id)
                    return

                elif state == _INSTANCE_STATE_FAILED:
                    reason = getattr(response.data, "lifecycle_details", "unknown failure")
                    logs = await self._fetch_instance_logs(instance_id)
                    error_msg = f"Container Instance failed: {reason}"
                    if logs:
                        error_msg += f"\n\n{logs}"
                    future.set_exception(RuntimeError(error_msg))

                    await self._delete_container_instance(instance_id)
                    return

                elif state in _INSTANCE_STATE_RUNNING:
                    continue
                else:
                    logging.warning(f"Unexpected Container Instance state: {state} for {instance_id[-20:]}")

            except Exception as poll_exc:
                logging.exception(f"Error polling Container Instance {instance_id[-20:]}: {poll_exc}")

    async def _delete_container_instance(self, instance_id: str) -> None:
        """Delete an OCI Container Instance (used for cancellation and post-completion cleanup)."""
        try:
            self._container_instances_client.delete_container_instance(container_instance_id=instance_id)
            logging.info(f"Deleted Container Instance {instance_id[-20:]}")
        except Exception as exc:
            logging.warning(f"Failed to delete Container Instance {instance_id[-20:]}: {exc}")

    async def _fetch_instance_logs(self, instance_id: str) -> str:
        """
        Fetch OCI Logging output for a failed container instance.

        Uses the OCI Logging Search service to retrieve recent log records
        for the container instance.
        """
        try:
            import oci

            config = oci.config.from_file(profile_name=self._oci_config_profile)
            log_search_client = oci.loggingsearch.LogSearchClient(config)

            # Search for logs associated with this container instance
            search_details = oci.loggingsearch.models.SearchLogsDetails(
                time_start=None,  # last N minutes handled by API default
                time_end=None,
                search_query=(
                    f'search "{self._compartment_id}" | '
                    f'where subject = "{instance_id}" | '
                    f"sort by datetime desc | limit 100"
                ),
                is_return_field_info=False,
            )

            response = log_search_client.search_logs(search_logs_details=search_details)
            results = response.data.results or []

            if not results:
                return "(No log records found)"

            lines = [str(r.data) for r in results if r.data]
            return "Container Instance logs:\n" + "\n".join(lines)

        except Exception as exc:
            logging.warning(f"Failed to fetch logs for instance {instance_id[-20:]}: {exc}")
            return f"(Failed to fetch logs: {exc})"

    @staticmethod
    def __get_task_priority(task: Task) -> int:
        """Get task priority from task metadata."""
        priority = retrieve_task_flags_from_task(task).priority

        if priority < 0:
            raise ValueError(f"invalid task priority, must be positive or zero, got {priority}")

        return priority
