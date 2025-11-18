import dataclasses
from typing import List, Optional, Tuple

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.event_loop import EventLoopType

try:
    from typing import override  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import override  # type: ignore[attr-defined]


@dataclasses.dataclass
class ECSWorkerAdapterConfig(ConfigClass):
    # Server (adapter) configuration
    adapter_web_host: str = dataclasses.field(
        metadata=dict(required=True, help="host address for the ecs worker adapter HTTP server")
    )
    adapter_web_port: int = dataclasses.field(
        metadata=dict(short="-p", required=True, help="port for the ecs worker adapter HTTP server")
    )

    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(nargs="?", help="scheduler address to connect workers to")
    )

    object_storage_address: Optional[ObjectStorageConfig] = dataclasses.field(
        default=None,
        metadata=dict(short="-osa", help="specify the object storage server address, e.g.: tcp://localhost:2346"),
    )

    # AWS / ECS specific configuration
    aws_access_key_id: Optional[str] = dataclasses.field(
        default=None, metadata=dict(env_var="AWS_ACCESS_KEY_ID", help="AWS access key id")
    )
    aws_secret_access_key: Optional[str] = dataclasses.field(
        default=None, metadata=dict(env_var="AWS_SECRET_ACCESS_KEY", help="AWS secret access key")
    )
    aws_region: str = dataclasses.field(default="us-east-1", metadata=dict(help="AWS region for ECS cluster"))
    ecs_subnets: List[str] = dataclasses.field(
        default_factory=list,
        metadata=dict(
            type=lambda s: [x for x in s.split(",") if x],
            required=True,
            help="Comma-separated list of AWS subnet IDs for ECS tasks",
        ),
    )
    ecs_cluster: str = dataclasses.field(default="scaler-cluster", metadata=dict(help="ECS cluster name"))
    ecs_task_image: str = dataclasses.field(
        default="public.ecr.aws/v4u8j8r6/scaler:latest", metadata=dict(help="Container image used for ECS tasks")
    )
    ecs_python_requirements: str = dataclasses.field(
        default="tomli;pargraph;parfun;pandas", metadata=dict(help="Python requirements string passed to the ECS task")
    )
    ecs_python_version: str = dataclasses.field(default="3.12.11", metadata=dict(help="Python version for ECS task"))
    ecs_task_definition: str = dataclasses.field(
        default="scaler-task-definition", metadata=dict(help="ECS task definition")
    )
    ecs_task_cpu: int = dataclasses.field(
        default=4, metadata=dict(help="Number of vCPUs for task (used to derive worker count)")
    )
    ecs_task_memory: int = dataclasses.field(default=30, metadata=dict(help="Task memory in GB for Fargate"))

    # Generic worker adapter options
    io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS, metadata=dict(short="-it", help="number of io threads for zmq")
    )
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string(""),
        metadata=dict(
            short="-pwc", help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")'
        ),
    )
    worker_task_queue_size: int = dataclasses.field(
        default=defaults.DEFAULT_PER_WORKER_QUEUE_SIZE, metadata=dict(short="-wtqs", help="specify worker queue size")
    )
    max_instances: int = dataclasses.field(
        default=defaults.DEFAULT_NUMBER_OF_WORKER,
        metadata=dict(
            short="-mi",
            help="maximum number of ECS task instances that can be started, "
            "required to avoid unexpected surprise bills, -1 means no limit",
        ),
    )
    heartbeat_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        metadata=dict(short="-hi", help="number of seconds that worker agent send heartbeat to scheduler"),
    )
    task_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_TASK_TIMEOUT_SECONDS,
        metadata=dict(short="-tt", help="default task timeout seconds, 0 means never timeout"),
    )
    death_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT,
        metadata=dict(short="-dt", help="number of seconds without scheduler contact before worker shuts down"),
    )
    garbage_collect_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        metadata=dict(short="-gc", help="number of seconds worker doing garbage collection"),
    )
    trim_memory_threshold_bytes: int = dataclasses.field(
        default=defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        metadata=dict(
            short="-tm", help="number of bytes threshold for worker process that trigger deep garbage collection"
        ),
    )
    hard_processor_suspend: bool = dataclasses.field(
        default=defaults.DEFAULT_HARD_PROCESSOR_SUSPEND,
        metadata=dict(
            short="-hps",
            action="store_true",
            help="if true, suspended worker's processors will be actively suspended with a SIGTSTP signal",
        ),
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-e", choices=EventLoopType.allowed_types(), help="select event loop type"),
    )
    logging_paths: Tuple[str, ...] = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_PATHS,
        metadata=dict(
            short="-lp", nargs="*", help="specify where worker logs should be logged to, it can accept multiple files"
        ),
    )
    logging_level: str = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_LEVEL,
        metadata=dict(
            short="-ll", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), help="specify the logging level"
        ),
    )
    logging_config_file: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-lc", help="use standard python .conf file to specify python logging file configuration format"
        ),
    )

    def __post_init__(self):
        # Validate server fields
        if not (1 <= self.adapter_web_port <= 65535):
            raise ValueError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")

        # Validate numeric and collection values
        if self.io_threads <= 0:
            raise ValueError("io_threads must be a positive integer.")
        if self.worker_task_queue_size <= 0:
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

    @override
    @staticmethod
    def section_name() -> str:
        return "ecs_worker_adapter"

    @override
    @staticmethod
    def program_name() -> str:
        return "scaler ECS worker adapter"
