import dataclasses
from typing import Optional, Tuple

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.config.types.zmq import ZMQConfig
from scaler.config import defaults
from scaler.utility.logging.utility import LoggingLevel


@dataclasses.dataclass
class SymphonyWorkerConfig:
    scheduler_address: ZMQConfig
    object_storage_address: Optional[ObjectStorageConfig]
    service_name: str
    base_concurrency: int = defaults.DEFAULT_NUMBER_OF_WORKER
    worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string("")
    )
    server_http_host: str = "localhost"
    server_http_port: int = 0
    io_threads: int = defaults.DEFAULT_IO_THREADS
    worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    heartbeat_interval: int = defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    death_timeout_seconds: int = defaults.DEFAULT_WORKER_DEATH_TIMEOUT
    event_loop: str = "builtin"
    logging_paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    logging_level: str = defaults.DEFAULT_LOGGING_LEVEL
    logging_config_file: Optional[str] = None

    def __post_init__(self):
        """Validates configuration values after initialization."""
        if (
            self.base_concurrency <= 0
            or self.worker_task_queue_size <= 0
            or self.heartbeat_interval <= 0
            or self.death_timeout_seconds <= 0
            or self.io_threads <= 0
        ):
            raise ValueError("All concurrency, queue size, timeout, and thread count values must be positive integers.")

        if not self.service_name:
            raise ValueError("service_name cannot be an empty string.")

        valid_levels = {level.name for level in LoggingLevel}
        if self.logging_level.upper() not in valid_levels:
            raise ValueError(f"logging_level must be one of {valid_levels}, but got '{self.logging_level}'")
