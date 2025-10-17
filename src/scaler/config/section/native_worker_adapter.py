import dataclasses
from typing import Optional, Tuple

from scaler.config import defaults
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class NativeWorkerAdapterConfig:
    scheduler_address: ZMQConfig
    object_storage_address: Optional[ObjectStorageConfig] = None
    adapter_web_host: str = "localhost"
    adapter_web_port: int = 8080
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string("")
    )
    io_threads: int = defaults.DEFAULT_IO_THREADS
    worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    max_workers: int = defaults.DEFAULT_NUMBER_OF_WORKER
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
        if not isinstance(self.adapter_web_host, str):
            raise TypeError(f"adapter_web_host should be string, given {self.adapter_web_host}")
        if not isinstance(self.adapter_web_port, int):
            raise TypeError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")
        if self.io_threads <= 0:
            raise ValueError("io_threads must be a positive integer.")
        if self.worker_task_queue_size <= 0:
            raise ValueError("worker_task_queue_size must be positive.")
        if self.heartbeat_interval_seconds <= 0 or self.task_timeout_seconds < 0 or self.death_timeout_seconds <= 0:
            raise ValueError("All interval/timeout second values must be positive.")
