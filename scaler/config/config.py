import abc
import argparse
import dataclasses
import enum
import re
from urllib.parse import urlparse

try:
    import tomllib  # Python 3.11+
except ImportError:
    import tomli as tomllib
from typing import Dict, List, Optional, Tuple, Type, Union, get_args, get_origin
from typing_extensions import Self

from scaler.io import config as defaults
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.utility.logging.utility import LoggingLevel


# ======================================================================================
# Helper Functions & APIs
# ======================================================================================


def parse_capabilities(capability_string: str) -> Dict[str, int]:
    """Parses a capability string like 'linux,cpu=4' into a dictionary."""
    capabilities: Dict[str, int] = {}
    if not capability_string:
        return capabilities
    for item in capability_string.split(","):
        name, _, value = item.partition("=")
        if value:
            capabilities[name.strip()] = int(value)
        else:
            capabilities[name.strip()] = -1
    return capabilities


class ZMQType(enum.Enum):
    inproc = "inproc"
    ipc = "ipc"
    tcp = "tcp"

    @staticmethod
    def allowed_types():
        return {t.value for t in ZMQType}


# ======================================================================================
# Base classes for the configuration system
# ======================================================================================


class ConfigValue(metaclass=abc.ABCMeta):
    """A base class for composite config values that can be parsed and serialized from/to a string."""

    @classmethod
    @abc.abstractmethod
    def from_string(cls, value: str) -> Self:
        pass

    @abc.abstractmethod
    def __str__(self) -> str:
        pass


class Config:
    """Base class for main configuration objects."""

    @classmethod
    def get_config(cls: Type[Self], config_path: Optional[str], args: argparse.Namespace) -> Self:
        """
        Loads configuration from a TOML file and overrides it with command-line arguments.

        1. Loads defaults from the specified TOML file (if any).
        2. Overrides TOML values with any non-None values from the parsed argparse Namespace.
        3. Converts string values to custom ConfigValue types where necessary.
        4. Instantiates and returns the configuration dataclass.
        """
        if not dataclasses.is_dataclass(cls):
            raise TypeError(f"{cls.__name__} is not a dataclass and cannot be used with this config loader.")
        config_from_file = {}
        if config_path:
            try:
                with open(config_path, "rb") as f:
                    config_from_file = tomllib.load(f)
            except FileNotFoundError:
                raise FileNotFoundError(f"Configuration file not found at: {config_path}")

        config_from_args = {k: v for k, v in vars(args).items() if v is not None}

        merged_config_data = {**config_from_file, **config_from_args}

        valid_keys = {f.name for f in dataclasses.fields(cls)}
        unknown_keys = set(merged_config_data.keys()) - valid_keys - {"config"}
        if unknown_keys:
            raise ValueError(f"Unknown configuration key(s) for {cls.__name__}: {', '.join(unknown_keys)}")

        final_kwargs = {}
        for field in dataclasses.fields(cls):
            if field.name in merged_config_data:
                raw_value = merged_config_data[field.name]

                field_type = field.type
                is_optional = get_origin(field_type) is Union
                if is_optional:
                    possible_types = [t for t in get_args(field_type) if t is not type(None)]
                    actual_type = possible_types[0] if possible_types else field_type
                else:
                    actual_type = field_type

                if (
                    isinstance(raw_value, str)
                    and isinstance(actual_type, type)
                    and issubclass(actual_type, ConfigValue)
                    and not isinstance(raw_value, actual_type)
                ):
                    final_kwargs[field.name] = actual_type.from_string(raw_value)
                elif (
                    isinstance(raw_value, str) and isinstance(actual_type, type) and issubclass(actual_type, enum.Enum)
                ):
                    try:
                        final_kwargs[field.name] = actual_type[raw_value]
                    except KeyError as e:
                        raise ValueError(f"'{raw_value}' is not a valid member for {actual_type.__name__}") from e
                elif isinstance(raw_value, list) and get_origin(field.type) is tuple:
                    final_kwargs[field.name] = tuple(raw_value)
                else:
                    final_kwargs[field.name] = raw_value

        try:
            return cls(**final_kwargs)
        except TypeError as e:
            missing_fields = [
                f.name
                for f in dataclasses.fields(cls)
                if f.init
                and f.name not in final_kwargs
                and f.default is dataclasses.MISSING
                and f.default_factory is dataclasses.MISSING
            ]
            if missing_fields:
                raise TypeError(
                    f"Missing required configuration arguments: {', '.join(missing_fields)}. "
                    f"Please provide them via command line or a TOML config file."
                ) from e
            else:
                raise e


# ======================================================================================
# Concrete ConfigValue implementations
# ======================================================================================


@dataclasses.dataclass
class ZMQConfig(ConfigValue):
    type: ZMQType
    host: str
    port: Optional[int] = None

    def __post_init__(self):
        if not isinstance(self.type, ZMQType):
            raise TypeError(f"Invalid zmq type {self.type}, available types are: {ZMQType.allowed_types()}")

        if not isinstance(self.host, str):
            raise TypeError(f"Host should be string, given {self.host}")

        if self.port is None:
            if self.type == ZMQType.tcp:
                raise ValueError(f"type {self.type.value} should have `port`")
        else:
            if self.type in {ZMQType.inproc, ZMQType.ipc}:
                raise ValueError(f"type {self.type.value} should not have `port`")

            if not isinstance(self.port, int):
                raise TypeError(f"Port should be integer, given {self.port}")

    def to_address(self):
        if self.type == ZMQType.tcp:
            return f"tcp://{self.host}:{self.port}"

        if self.type in {ZMQType.inproc, ZMQType.ipc}:
            return f"{self.type.value}://{self.host}"

        raise TypeError(f"Unsupported ZMQ type: {self.type}")

    @classmethod
    def from_string(cls, value: str) -> Self:
        if "://" not in value:
            raise ValueError("valid ZMQ config should be like tcp://127.0.0.1:12345")

        socket_type, host_port = value.split("://", 1)
        if socket_type not in ZMQType.allowed_types():
            raise ValueError(f"supported ZMQ types are: {ZMQType.allowed_types()}")

        socket_type_enum = ZMQType(socket_type)
        if socket_type_enum in {ZMQType.inproc, ZMQType.ipc}:
            host = host_port
            port_int = None
        elif socket_type_enum == ZMQType.tcp:
            host, port = host_port.split(":")
            try:
                port_int = int(port)
            except ValueError:
                raise ValueError(f"cannot convert '{port}' to port number")
        else:
            raise ValueError(f"Unsupported ZMQ type: {socket_type}")

        return cls(socket_type_enum, host, port_int)

    def __str__(self) -> str:
        return self.to_address()

    def __repr__(self) -> str:
        return self.to_address()


@dataclasses.dataclass
class ObjectStorageConfig(ConfigValue):
    host: str
    port: int
    identity: str = "ObjectStorageServer"

    def __post_init__(self):
        if not isinstance(self.host, str):
            raise TypeError(f"Host should be string, given {self.host}")

        if not isinstance(self.identity, str):
            raise TypeError(f"Identity should be string, given {self.identity}")

        if not isinstance(self.port, int):
            raise TypeError(f"Port should be integer, given {self.port}")

    def __str__(self) -> str:
        return self.to_string()

    def to_string(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    @classmethod
    def from_string(cls, value: str) -> "ObjectStorageConfig":
        address_format = r"^tcp://([a-zA-Z0-9\.\-]+):([0-9]{1,5})$"
        match = re.compile(address_format).match(value)

        if not match:
            raise ValueError("object storage address has to be tcp://<host>:<port>")

        host = match.group(1)
        port = int(match.group(2))

        return cls(host=host, port=port)


@dataclasses.dataclass
class WorkerNames(ConfigValue):
    """Parses a comma-separated string of worker names into a list."""

    names: List[str]

    @classmethod
    def from_string(cls, value: str) -> Self:
        if not value:
            return cls([])
        names = [name.strip() for name in value.split(",")]
        return cls(names)

    def __str__(self) -> str:
        return ",".join(self.names)

    def __len__(self) -> int:
        return len(self.names)


@dataclasses.dataclass
class WorkerCapabilities(ConfigValue):
    """Parses a string of worker capabilities using the centralized parse_capabilities function."""

    capabilities: Dict[str, int]

    @classmethod
    def from_string(cls, value: str) -> Self:
        if not value:
            return cls({})

        capabilities = parse_capabilities(value)
        return cls(capabilities)

    def __str__(self) -> str:
        items = []
        for name, cap in self.capabilities.items():
            if cap == -1:
                items.append(name)
            else:
                items.append(f"{name}={cap}")
        return ",".join(items)


# ======================================================================================
# Application Configuration Dataclasses
# ======================================================================================


@dataclasses.dataclass
class ObjectStorageServerConfig(Config):
    object_storage_address: ObjectStorageConfig


@dataclasses.dataclass
class SchedulerConfig(Config):
    scheduler_address: ZMQConfig = dataclasses.field()
    storage_address: Optional[ObjectStorageConfig] = None
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


@dataclasses.dataclass
class ClusterConfig(Config):
    scheduler_address: ZMQConfig
    storage_address: Optional[ObjectStorageConfig] = None
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


@dataclasses.dataclass
class WebUIConfig(Config):
    monitor_address: ZMQConfig
    web_host: str = "0.0.0.0"
    web_port: int = 50001

    def __post_init__(self):
        if not isinstance(self.web_host, str):
            raise TypeError(f"Web host should be string, given {self.web_host}")
        if not isinstance(self.web_port, int):
            raise ValueError(f"Web port should be an integer, given {self.web_port}")


@dataclasses.dataclass
class NativeAdapterConfig(Config):
    scheduler_address: ZMQConfig
    storage_address: Optional[ObjectStorageConfig] = None
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
            raise ValueError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")
        if self.io_threads <= 0:
            raise ValueError("io_threads must be a positive integer.")
        if self.worker_task_queue_size <= 0:
            raise ValueError("worker_task_queue_size must be positive.")
        if self.heartbeat_interval_seconds <= 0 or self.task_timeout_seconds < 0 or self.death_timeout_seconds <= 0:
            raise ValueError("All interval/timeout second values must be positive.")


@dataclasses.dataclass
class TopConfig(Config):
    monitor_address: ZMQConfig
    timeout: int = 5

    def __post_init__(self):
        if self.timeout <= 0:
            raise ValueError("timeout must be a positive integer.")


@dataclasses.dataclass
class SymphonyWorkerConfig(Config):
    scheduler_address: ZMQConfig
    service_name: str
    base_concurrency: int = defaults.DEFAULT_NUMBER_OF_WORKER
    worker_name: Optional[str] = None
    worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string("")
    )
    worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    heartbeat_interval_seconds: int = defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    death_timeout_seconds: int = defaults.DEFAULT_WORKER_DEATH_TIMEOUT
    event_loop: str = "builtin"
    io_threads: int = defaults.DEFAULT_IO_THREADS
    logging_paths: Tuple[str, ...] = defaults.DEFAULT_LOGGING_PATHS
    logging_level: str = defaults.DEFAULT_LOGGING_LEVEL
    logging_config_file: Optional[str] = None

    def __post_init__(self):
        """Validates configuration values after initialization."""
        if (
            self.base_concurrency <= 0
            or self.worker_task_queue_size <= 0
            or self.heartbeat_interval_seconds <= 0
            or self.death_timeout_seconds <= 0
            or self.io_threads <= 0
        ):
            raise ValueError("All concurrency, queue size, timeout, and thread count values must be positive integers.")

        if not self.service_name:
            raise ValueError("service_name cannot be an empty string.")

        if self.worker_name is not None and not self.worker_name:
            raise ValueError("If provided, worker_name cannot be an empty string.")

        valid_levels = {level.name for level in LoggingLevel}
        if self.logging_level.upper() not in valid_levels:
            raise ValueError(f"logging_level must be one of {valid_levels}, but got '{self.logging_level}'")
