import dataclasses
from typing import Optional, Tuple

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
class NativeWorkerAdapterConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, nargs="?", help="scheduler address to connect workers to")
    )

    object_storage_address: Optional[ObjectStorageConfig] = dataclasses.field(
        default=None,
        metadata=dict(short="-osa", help="specify the object storage server address, e.g.: tcp://localhost:2346"),
    )

    # Server (adapter) configuration
    adapter_web_host: str = dataclasses.field(
        default="localhost", metadata=dict(help="host address for the ecs worker adapter HTTP server")
    )
    adapter_web_port: int = dataclasses.field(
        default=8080, metadata=dict(short="-p", help="port for the ecs worker adapter HTTP server")
    )

    # Generic worker adapter options
    io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS, metadata=dict(help="number of io threads for zmq")
    )
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string(""),
        metadata=dict(
            short="-pwc", help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")'
        ),
    )
    worker_task_queue_size: int = dataclasses.field(
        default=10, metadata=dict(short="-wtqs", help="specify worker queue size")
    )
    max_workers: int = dataclasses.field(
        default=defaults.DEFAULT_NUMBER_OF_WORKER,
        metadata=dict(short="-mw", help="maximum number of workers that can be started, -1 means no limit"),
    )
    heartbeat_interval: int = dataclasses.field(
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

    @override
    @staticmethod
    def section_name() -> str:
        return "native_worker_adapter"

    @override
    @staticmethod
    def program_name() -> str:
        return "scaler_native_worker_adapter"

    def __post_init__(self):
        if not (1 <= self.adapter_web_port <= 65535):
            raise TypeError(f"adapter_web_port must be between 1 and 65535, but got {self.adapter_web_port}")
        if self.io_threads <= 0:
            raise ValueError("io_threads must be a positive integer.")
        if self.worker_task_queue_size <= 0:
            raise ValueError("worker_task_queue_size must be positive.")
        if self.heartbeat_interval <= 0 or self.task_timeout_seconds < 0 or self.death_timeout_seconds <= 0:
            raise ValueError("All interval/timeout second values must be positive.")
