import copy
import dataclasses
import logging
import tomli as tomllib
import re
from argparse import Namespace
from dataclasses import InitVar, fields, is_dataclass
from typing import Any, Dict, Protocol, Type, TypeVar, List, Optional, cast, Tuple
from typing_extensions import Self
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.io import config as defaults
from scaler.utility.zmq_config import ZMQConfig, ZMQType


@dataclasses.dataclass
class Config:
    """A base class for all configuration dataclasses."""

    def describe(self) -> str:
        lines = []
        for parent_field in dataclasses.fields(self):
            parent_name = parent_field.name
            nested_obj = getattr(self, parent_name)
            if dataclasses.is_dataclass(nested_obj):
                for child_field in dataclasses.fields(nested_obj):
                    key = f"{parent_name}.{child_field.name}"
                    value = getattr(nested_obj, child_field.name)
                    lines.append(f'{key} = "{value}"')
        return "\n".join(lines)

    @classmethod
    def get_config(cls: Type[Self], filename: Optional[str] = None, args: Optional[Namespace] = None) -> Self:
        """
        Loads configuration from a TOML file and overrides it with command-line arguments.
        This method is "smart": it allows unprefixed arguments (e.g., --web_port) when they
        are unique, and only requires a prefix (e.g., --worker_name) when a name is ambiguous.
        """
        toml_data = {}
        if filename:
            try:
                with open(filename, "rb") as f:
                    toml_data = tomllib.load(f)
                    logging.info(f"Successfully loaded configuration from {filename}")
            except FileNotFoundError:
                logging.warning(
                    f"Configuration file not found at: {filename}. Proceeding with defaults and command-line args."
                )
            except tomllib.TOMLDecodeError as e:
                logging.error(f"Error decoding TOML file at {filename}: {e}")
                raise

        args_data: dict[str, Any] = {}
        if args:
            raw_args = {k: v for k, v in vars(args).items() if v is not None}
            # Map section names to their dataclass types (e.g., 'worker': WorkerConfig)
            config_sections = {f.name: f.type for f in dataclasses.fields(cls) if is_dataclass(f.type)}

            for key, value in raw_args.items():
                # Check for an explicit prefix first (e.g., scheduler_address_str)
                found_section = False
                for section_name in config_sections:
                    prefix = f"{section_name}_"
                    if key.startswith(prefix):
                        field_name = key[len(prefix) :]
                        if section_name not in args_data:
                            args_data[section_name] = {}
                        args_data[section_name][field_name] = value
                        found_section = True
                        break
                if found_section:
                    continue

                # If no prefix, find which sections contain this field name
                matching_sections = []
                for section_name, section_type in config_sections.items():
                    # Check if the key exists as a field in the nested dataclass
                    if hasattr(section_type, "__dataclass_fields__") and key in section_type.__dataclass_fields__:
                        matching_sections.append(section_name)

                if len(matching_sections) == 1:
                    # The key is unique to one section. Assign it.
                    section_name = matching_sections[0]
                    if section_name not in args_data:
                        args_data[section_name] = {}
                    args_data[section_name][key] = value
                elif len(matching_sections) > 1:
                    logging.warning(
                        f"Command-line argument '{key}' is ambiguous (exists in {matching_sections}). "
                        f"Please use a prefix, e.g., '{matching_sections[0]}_{key}'. Argument ignored."
                    )
                else:
                    logging.warning(f"Command-line argument '{key}' did not match any config field and was ignored.")

        # Merge TOML data with command-line arguments, prioritizing args
        final_data = copy.deepcopy(toml_data)
        _deep_merge(args_data, final_data)

        return _from_dict(cls, final_data)  # type: ignore [type-var]


@dataclasses.dataclass
class ObjectStorageConfig:
    host: str = "127.0.0.1"
    port: int = 6380
    address: ZMQConfig = dataclasses.field(init=False)

    def to_string(self) -> str:
        return f"tcp://{self.host}:{self.port}"

    @staticmethod
    def from_string(address: str) -> "ObjectStorageConfig":
        address_format = r"^tcp://([a-zA-Z0-9\.\-]+):([0-9]{1,5})$"
        match = re.compile(address_format).match(address)
        if not match:
            raise ValueError("object storage address has to be tcp://<host>:<port>")
        host = match.group(1)
        port = int(match.group(2))
        return ObjectStorageConfig(host=host, port=port)

    def __post_init__(self):
        self.address = ZMQConfig(type=ZMQType.tcp, host=self.host, port=self.port)


@dataclasses.dataclass
class SchedulerConfig:
    address_str: InitVar[str] = "tcp://127.0.0.1:6378"
    storage_address_str: InitVar[Optional[str]] = "tcp://127.0.0.1:6379"
    monitor_address_str: InitVar[Optional[str]] = "tcp://127.0.0.1:6380"
    adapter_webhook_url: Optional[str] = None
    address: ZMQConfig = dataclasses.field(init=False)
    storage_address: Optional[ObjectStorageConfig] = dataclasses.field(init=False)
    monitor_address: Optional[ZMQConfig] = dataclasses.field(init=False)
    protected: bool = True
    allocate_policy_str: InitVar[str] = "even"
    allocate_policy: AllocatePolicy = dataclasses.field(init=False)
    event_loop: str = "builtin"
    zmq_io_threads: int = defaults.DEFAULT_IO_THREADS
    max_number_of_tasks_waiting: int = defaults.DEFAULT_MAX_NUMBER_OF_TASKS_WAITING
    client_timeout_seconds: int = defaults.DEFAULT_CLIENT_TIMEOUT_SECONDS
    worker_timeout_seconds: int = defaults.DEFAULT_WORKER_TIMEOUT_SECONDS
    object_retention_seconds: int = defaults.DEFAULT_OBJECT_RETENTION_SECONDS
    load_balance_seconds: int = defaults.DEFAULT_LOAD_BALANCE_SECONDS
    load_balance_trigger_times: int = defaults.DEFAULT_LOAD_BALANCE_TRIGGER_TIMES

    def __post_init__(
        self,
        address_str: str,
        storage_address_str: Optional[str],
        monitor_address_str: Optional[str],
        allocate_policy_str: str,
    ):
        self.address = ZMQConfig.from_string(address_str)
        self.storage_address = ObjectStorageConfig.from_string(storage_address_str) if storage_address_str else None
        self.monitor_address = ZMQConfig.from_string(monitor_address_str) if monitor_address_str else None
        self.allocate_policy = AllocatePolicy[allocate_policy_str]


@dataclasses.dataclass
class WorkerConfig:
    name: str = "DefaultWorker"
    io_threads: int = defaults.DEFAULT_IO_THREADS
    per_worker_task_queue_size: int = defaults.DEFAULT_PER_WORKER_QUEUE_SIZE
    heartbeat_interval_seconds: int = defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS
    garbage_collect_interval_seconds: int = defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS
    trim_memory_threshold_bytes: int = defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES
    task_timeout_seconds: int = defaults.DEFAULT_TASK_TIMEOUT_SECONDS
    death_timeout_seconds: int = defaults.DEFAULT_WORKER_DEATH_TIMEOUT
    hard_processor_suspend: bool = defaults.DEFAULT_HARD_PROCESSOR_SUSPEND


@dataclasses.dataclass
class ClusterConfig:
    worker_names_str: InitVar[str] = "worker"
    per_worker_capabilities_str: InitVar[str] = ""
    num_of_workers: int = defaults.DEFAULT_NUMBER_OF_WORKER
    worker_names: List[str] = dataclasses.field(init=False)
    per_worker_capabilities: Dict[str, int] = dataclasses.field(init=False)

    def __post_init__(self, worker_names_str: str, per_worker_capabilities_str: str):
        if per_worker_capabilities_str:
            from scaler.entry_points.cluster import parse_capabilities
            self.per_worker_capabilities = parse_capabilities(per_worker_capabilities_str)
        else:
            self.per_worker_capabilities = {}
        if worker_names_str:
            self.worker_names = list(name.strip() for name in worker_names_str.split(","))
        else:
            self.worker_names = list()


@dataclasses.dataclass
class WebUIConfig:
    web_host: str = "0.0.0.0"
    web_port: int = 50001
    sched_address: str = "tcp://0.0.0.0:6379"


@dataclasses.dataclass
class NativeAdapterConfig:
    adapter_web_host: str = "localhost"
    adapter_web_port: int = 8080


@dataclasses.dataclass
class LoggingConfig:
    paths: Tuple[str] = defaults.DEFAULT_LOGGING_PATHS
    level: str = defaults.DEFAULT_LOGGING_LEVEL
    config_file: Optional[str] = None


@dataclasses.dataclass
class ScalerConfig(Config):
    scheduler: SchedulerConfig = dataclasses.field(default_factory=SchedulerConfig)
    worker: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    webui: WebUIConfig = dataclasses.field(default_factory=WebUIConfig)
    native_adapter: NativeAdapterConfig = dataclasses.field(default_factory=NativeAdapterConfig)
    object_storage: ObjectStorageConfig = dataclasses.field(default_factory=ObjectStorageConfig)
    cluster: ClusterConfig = dataclasses.field(default_factory=ClusterConfig)
    logging: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)


def _deep_merge(source: dict, destination: dict) -> dict:
    """
    Recursively merges the `source` dictionary into the `destination` dictionary.
    Values from `source` will overwrite values in `destination`.
    """
    for key, value in source.items():
        if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
            destination[key] = _deep_merge(value, destination[key])
        else:
            destination[key] = value
    return destination


class _DataclassProtocol(Protocol):
    __dataclass_fields__: dict


DataclassType = TypeVar("DataclassType", bound=_DataclassProtocol)


def _from_dict(cls: Type[DataclassType], data: dict) -> DataclassType:
    """
    Constructs a dataclass instance from a dictionary, correctly handling nested dataclasses.
    """

    # First, create any nested dataclass objects.
    nested_objects = {}
    for field in fields(cls):  # type: ignore
        if is_dataclass(field.type) and field.name in data:
            nested_objects[field.name] = _from_dict(cast(Type[DataclassType], field.type), data[field.name])

    # Now, create the top-level dataclass object.
    kwargs = {**data, **nested_objects}
    return cls(**kwargs)
