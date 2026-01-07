import uuid
from typing import Dict, Optional, Tuple

from aiohttp import web
from aiohttp.web_request import Request

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.identifiers import WorkerID
from scaler.worker_adapter.common import CapacityExceededError, WorkerGroupID, WorkerGroupNotFoundError
from scaler.worker_adapter.aws_batch.worker import AWSBatchWorker


class AWSBatchWorkerAdapter:
    """
    AWS Batch Worker Adapter that manages worker groups using AWS Batch jobs.
    Similar to Symphony adapter but uses AWS Batch for task execution.
    """

    def __init__(
        self,
        address: ZMQConfig,
        object_storage_address: Optional[ObjectStorageConfig],
        # AWS Batch specific parameters
        job_queue: str,
        job_definition: str,
        aws_region: str,
        aws_access_key_id: Optional[str],
        aws_secret_access_key: Optional[str],
        # Scaler parameters
        base_concurrency: int,
        capabilities: Dict[str, int],
        io_threads: int,
        task_queue_size: int,
        heartbeat_interval_seconds: int,
        death_timeout_seconds: int,
        event_loop: str,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
        # AWS Batch job parameters
        vcpus: int = 1,
        memory: int = 2048,
        max_worker_groups: int = 10,
    ):
        self._address = address
        self._object_storage_address = object_storage_address
        self._job_queue = job_queue
        self._job_definition = job_definition
        self._aws_region = aws_region
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._base_concurrency = base_concurrency
        self._capabilities = capabilities
        self._io_threads = io_threads
        self._task_queue_size = task_queue_size
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._death_timeout_seconds = death_timeout_seconds
        self._event_loop = event_loop
        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file
        self._vcpus = vcpus
        self._memory = memory
        self._max_worker_groups = max_worker_groups

        # Track active worker groups: worker_group_id -> {worker_id -> AWSBatchWorker}
        self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, AWSBatchWorker]] = {}

        # Initialize AWS Batch client
        self._batch_client = None
        self._initialize_aws_batch_client()

    def _initialize_aws_batch_client(self):
        """Initialize AWS Batch client with credentials and region."""
        # TODO: Initialize boto3 Batch client with proper credentials
        # Should handle IAM roles, access keys, or default credential chain
        raise NotImplementedError("AWS Batch client initialization is not yet implemented")

    async def start_worker_group(self) -> WorkerGroupID:
        """
        Start a new worker group by submitting an AWS Batch job.
        
        Returns:
            WorkerGroupID: Unique identifier for the created worker group
            
        Raises:
            CapacityExceededError: If maximum worker groups limit is reached
        """
        if len(self._worker_groups) >= self._max_worker_groups:
            raise CapacityExceededError(f"Maximum number of worker groups ({self._max_worker_groups}) reached")

        # TODO: Implement AWS Batch job submission
        # 1. Generate unique job name
        # 2. Create job parameters with Scaler configuration
        # 3. Submit job to AWS Batch queue
        # 4. Create AWSBatchWorker instance to track the job
        # 5. Store worker group mapping
        raise NotImplementedError("AWS Batch worker group creation is not yet implemented")

    async def shutdown_worker_group(self, worker_group_id: WorkerGroupID):
        """
        Shutdown a worker group by terminating the AWS Batch job.
        
        Args:
            worker_group_id: ID of the worker group to shutdown
            
        Raises:
            WorkerGroupNotFoundError: If worker group ID doesn't exist
        """
        if worker_group_id not in self._worker_groups:
            raise WorkerGroupNotFoundError(f"Worker group with ID {worker_group_id.decode()} does not exist")

        # TODO: Implement AWS Batch job termination
        # 1. Get job ID from worker group
        # 2. Cancel/terminate the AWS Batch job
        # 3. Clean up worker group tracking
        # 4. Handle graceful shutdown vs force termination
        raise NotImplementedError("AWS Batch worker group shutdown is not yet implemented")

    def _create_batch_job_parameters(self, worker_group_id: WorkerGroupID) -> dict:
        """
        Create AWS Batch job parameters for Scaler worker.
        
        Args:
            worker_group_id: ID of the worker group being created
            
        Returns:
            dict: AWS Batch job submission parameters
        """
        # TODO: Build job parameters including:
        # - Job name with worker group ID
        # - Environment variables for Scaler configuration
        # - Resource requirements (vCPUs, memory)
        # - Container overrides for Scaler worker command
        # - Network configuration if needed
        raise NotImplementedError("AWS Batch job parameter creation is not yet implemented")

    def _monitor_batch_job_status(self, job_id: str) -> str:
        """
        Monitor AWS Batch job status.
        
        Args:
            job_id: AWS Batch job ID to monitor
            
        Returns:
            str: Current job status (SUBMITTED, PENDING, RUNNABLE, STARTING, RUNNING, SUCCEEDED, FAILED)
        """
        # TODO: Query AWS Batch for job status
        # Handle different job states and transitions
        raise NotImplementedError("AWS Batch job status monitoring is not yet implemented")

    async def webhook_handler(self, request: Request):
        """
        Handle webhook requests for worker adapter management.
        Supports OpenGRIS standard worker adapter API.
        """
        try:
            request_json = await request.json()
        except Exception:
            return web.json_response({"error": "Invalid JSON"}, status=400)

        if "action" not in request_json:
            return web.json_response({"error": "No action specified"}, status=400)

        action = request_json["action"]

        if action == "get_worker_adapter_info":
            return await self._handle_get_info()
        elif action == "start_worker_group":
            return await self._handle_start_worker_group()
        elif action == "shutdown_worker_group":
            return await self._handle_shutdown_worker_group(request_json)
        else:
            return web.json_response({"error": "Unknown action"}, status=400)

    async def _handle_get_info(self):
        """Handle get_worker_adapter_info request."""
        return web.json_response(
            {
                "max_worker_groups": self._max_worker_groups,
                "workers_per_group": 1,  # Each AWS Batch job = 1 worker group
                "base_capabilities": self._capabilities,
                "adapter_type": "aws_batch",
                "aws_region": self._aws_region,
                "job_queue": self._job_queue,
                "job_definition": self._job_definition,
            },
            status=200,
        )

    async def _handle_start_worker_group(self):
        """Handle start_worker_group request."""
        try:
            worker_group_id = await self.start_worker_group()
            return web.json_response(
                {
                    "status": "Worker group started",
                    "worker_group_id": worker_group_id.decode(),
                    "worker_ids": [worker_id.decode() for worker_id in self._worker_groups[worker_group_id].keys()],
                },
                status=200,
            )
        except CapacityExceededError as e:
            return web.json_response({"error": str(e)}, status=429)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_shutdown_worker_group(self, request_json: dict):
        """Handle shutdown_worker_group request."""
        if "worker_group_id" not in request_json:
            return web.json_response({"error": "No worker_group_id specified"}, status=400)

        worker_group_id = request_json["worker_group_id"].encode()
        try:
            await self.shutdown_worker_group(worker_group_id)
            return web.json_response({"status": "Worker group shutdown"}, status=200)
        except WorkerGroupNotFoundError as e:
            return web.json_response({"error": str(e)}, status=404)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    def create_app(self):
        """Create aiohttp web application for webhook handling."""
        app = web.Application()
        app.router.add_post("/", self.webhook_handler)
        return app