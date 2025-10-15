import dataclasses
from typing import Optional, Tuple
from urllib.parse import urlparse

from scaler.config import defaults
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.utility.logging.utility import LoggingLevel

from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.config.types.transport_type import TransportType


@dataclasses.dataclass
class SchedulerConfig:
    scheduler_address: ZMQConfig = dataclasses.field()
    object_storage_address: Optional[ObjectStorageConfig] = None
    monitor_address: Optional[ZMQConfig] = None
    adapter_webhook_url: Optional[str] = None
    protected: bool = True
    allocate_policy: AllocatePolicy = AllocatePolicy.even
    event_loop: str = "builtin"
    io_threads: int = defaults.DEFAULT_IO_THREADS
    max_number_of_tasks_waiting: int = defaults.DEFAULT_MAX_NUMBER_OF_TASKS_WAITING
    client_timeout_seconds: int = defaults.DEFAULT_CLIENT_TIMEOUT_SECONDS
    worker_timeout_seconds: int = defaults.DEFAULT_WORKER_TIMEOUT_SECONDS
    object_retention_seconds: int = defaults.DEFAULT_OBJECT_RETENTION_SECONDS
    load_balance_seconds: int = defaults.DEFAULT_LOAD_BALANCE_SECONDS
    load_balance_trigger_times: int = defaults.DEFAULT_LOAD_BALANCE_TRIGGER_TIMES
    logging_paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    logging_config_file: Optional[str] = None
    logging_level: str = defaults.DEFAULT_LOGGING_LEVEL
    transport_type: TransportType = defaults.DEFAULT_TRANSPORT_TYPE

    def __post_init__(self):
        if self.io_threads <= 0:
            raise ValueError("io_threads must be a positive integer.")
        if self.max_number_of_tasks_waiting < -1:
            raise ValueError("max_number_of_tasks_waiting must be -1 (for unlimited) or non-negative.")
        if (
            self.client_timeout_seconds <= 0
            or self.worker_timeout_seconds <= 0
            or self.object_retention_seconds <= 0
            or self.load_balance_seconds <= 0
        ):
            raise ValueError("All timeout/retention/balance second values must be positive.")
        if self.load_balance_trigger_times <= 0:
            raise ValueError("load_balance_trigger_times must be a positive integer.")
        if self.adapter_webhook_url:
            parsed_url = urlparse(self.adapter_webhook_url)
            if not all([parsed_url.scheme, parsed_url.netloc]):
                raise ValueError(f"adapter_webhook_url '{self.adapter_webhook_url}' is not a valid URL.")
        valid_levels = {level.name for level in LoggingLevel}
        if self.logging_level.upper() not in valid_levels:
            raise ValueError(f"logging_level must be one of {valid_levels}, but got '{self.logging_level}'")
