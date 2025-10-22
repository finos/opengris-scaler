import dataclasses
from typing import List, Optional, Tuple

from scaler.config import defaults
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.logging.utility import LoggingLevel


@dataclasses.dataclass
class ECSWorkerAdapterConfig:
    # Server (adapter) configuration
    adapter_web_host: str
    adapter_web_port: int

    scheduler_address: ZMQConfig
    object_storage_address: Optional[ObjectStorageConfig] = None

    # AWS / ECS specific configuration
    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None
    aws_region: str = "us-east-1"
    ecs_subnets: List[str] = dataclasses.field(default_factory=list)
    ecs_cluster: str = "scaler-cluster"
    ecs_task_image: str = "public.ecr.aws/v4u8j8r6/scaler:latest"
    ecs_python_requirements: str = "tomli;pargraph;parfun;pandas"
    ecs_python_version: str = "3.12.11"
    ecs_task_definition: str = "scaler-task-definition"
    ecs_task_cpu: int = 4
    ecs_task_memory: int = 30

    # Generic worker adapter options
    io_threads: int = defaults.DEFAULT_IO_THREADS
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string("")
    )
    per_worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    max_instances: int = defaults.DEFAULT_NUMBER_OF_WORKER
    heartbeat_interval_seconds: int = defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    task_timeout_seconds: int = defaults.DEFAULT_TASK_TIMEOUT_SECONDS
    death_timeout_seconds: int = defaults.DEFAULT_WORKER_DEATH_TIMEOUT
    garbage_collect_interval_seconds: int = defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS
    trim_memory_threshold_bytes: int = defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES
    hard_processor_suspend: bool = defaults.DEFAULT_HARD_PROCESSOR_SUSPEND
    event_loop: str = "builtin"
    logging_paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    logging_level: str = defaults.DEFAULT_LOGGING_LEVEL
    logging_config_file: Optional[str] = None

    def __post_init__(self):
        # Validate server fields
        if not isinstance(self.adapter_web_host, str):
            raise TypeError(f"adapter_web_host should be string, given {self.adapter_web_host}")
        if not isinstance(self.adapter_web_port, int) or not (1 <= self.adapter_web_port <= 65535):
            raise ValueError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")

        # Validate numeric and collection values
        if self.io_threads <= 0:
            raise ValueError("io_threads must be a positive integer.")
        if self.per_worker_task_queue_size <= 0:
            raise ValueError("worker_task_queue_size must be positive.")
        if self.ecs_task_cpu <= 0:
            raise ValueError("ecs_task_cpu must be a positive integer.")
        if self.ecs_task_memory <= 0:
            raise ValueError("ecs_task_memory must be a positive integer.")
        if self.heartbeat_interval_seconds <= 0 or self.death_timeout_seconds <= 0:
            raise ValueError("All interval/timeout second values must be positive.")
        if self.max_instances != -1 and self.max_instances <= 0:
            raise ValueError("max_instances must be -1 (no limit) or a positive integer.")
        if not isinstance(self.ecs_subnets, list) or len(self.ecs_subnets) == 0:
            raise ValueError("ecs_subnets must be a non-empty list of subnet ids.")

        # Validate required strings
        if not self.ecs_cluster:
            raise ValueError("ecs_cluster cannot be an empty string.")
        if not self.ecs_task_definition:
            raise ValueError("ecs_task_definition cannot be an empty string.")
        if not self.ecs_task_image:
            raise ValueError("ecs_task_image cannot be an empty string.")

        # Validate logging level
        valid_levels = {level.name for level in LoggingLevel}
        if self.logging_level.upper() not in valid_levels:
            raise ValueError(f"logging_level must be one of {valid_levels}, but got '{self.logging_level}'")
