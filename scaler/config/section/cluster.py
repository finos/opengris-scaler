import dataclasses
from typing import Optional, Tuple

from scaler.config import defaults
from scaler.utility.logging.utility import LoggingLevel

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.config.types.zmq import ZMQConfig
from scaler.config.types.transport_type import TransportType


@dataclasses.dataclass
class ClusterConfig:
    scheduler_address: ZMQConfig
    storage_address: Optional[ObjectStorageConfig] = None
    preload: Optional[str] = None
    worker_io_threads: int = defaults.DEFAULT_IO_THREADS
    worker_names: WorkerNames = dataclasses.field(default_factory=lambda: WorkerNames.from_string(""))
    num_of_workers: int = defaults.DEFAULT_NUMBER_OF_WORKER
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string("")
    )
    per_worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    heartbeat_interval_seconds: int = defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    task_timeout_seconds: int = defaults.DEFAULT_TASK_TIMEOUT_SECONDS
    death_timeout_seconds: int = defaults.DEFAULT_WORKER_DEATH_TIMEOUT
    garbage_collect_interval_seconds: int = defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS
    trim_memory_threshold_bytes: int = defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES
    hard_processor_suspend: bool = defaults.DEFAULT_HARD_PROCESSOR_SUSPEND
    event_loop: str = "builtin"
    transport_type: TransportType = defaults.DEFAULT_TRANSPORT_TYPE
    logging_paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    logging_config_file: Optional[str] = None
    logging_level: str = defaults.DEFAULT_LOGGING_LEVEL

    def __post_init__(self):
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
        if self.worker_names.names and len(self.worker_names.names) != self.num_of_workers:
            raise ValueError(
                f"The number of worker_names ({len(self.worker_names.names)}) \
                    must match num_of_workers ({self.num_of_workers})."
            )
        if self.per_worker_task_queue_size <= 0:
            raise ValueError("per_worker_task_queue_size must be positive.")
        if (
            self.heartbeat_interval_seconds <= 0
            or self.task_timeout_seconds < 0
            or self.death_timeout_seconds <= 0
            or self.garbage_collect_interval_seconds <= 0
        ):
            raise ValueError("All interval/timeout second values must be positive.")
        if self.trim_memory_threshold_bytes < 0:
            raise ValueError("trim_memory_threshold_bytes cannot be negative.")
        valid_levels = {level.name for level in LoggingLevel}
        if self.logging_level.upper() not in valid_levels:
            raise ValueError(f"logging_level must be one of {valid_levels}, but got '{self.logging_level}'")
