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
class SymphonyWorkerConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, nargs="?", help="scheduler address to connect workers to")
    )
    service_name: str = dataclasses.field(metadata=dict(short="-sn", help="symphony service name"))
    object_storage_address: Optional[ObjectStorageConfig] = dataclasses.field(
        default=None,
        metadata=dict(short="-osa", help="specify the object storage server address, e.g.: tcp://localhost:2346"),
    )
    base_concurrency: int = dataclasses.field(
        default=defaults.DEFAULT_NUMBER_OF_WORKER, metadata=dict(short="-n", help="base task concurrency")
    )
    worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities.from_string(""),
        metadata=dict(
            short="-wc", help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")'
        ),
    )
    adapter_web_host: str = dataclasses.field(
        default="localhost", metadata=dict(help="host address for the ecs worker adapter HTTP server")
    )
    adapter_web_port: int = dataclasses.field(
        default=0, metadata=dict(short="-p", help="port for the ecs worker adapter HTTP server")
    )
    io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS, metadata=dict(short="-it", help="number of io threads for zmq")
    )
    worker_task_queue_size: int = dataclasses.field(
        default=defaults.DEFAULT_PER_WORKER_QUEUE_SIZE, metadata=dict(short="-wtqs", help="specify worker queue size")
    )
    heartbeat_interval: int = dataclasses.field(
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        metadata=dict(short="-hi", help="number of seconds that worker agent send heartbeat to scheduler"),
    )
    death_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT,
        metadata=dict(short="-ds", help="number of seconds without scheduler contact before worker shuts down"),
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select event loop type"),
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

    @override
    @staticmethod
    def section_name() -> str:
        return "symphony_worker_adapter"

    @override
    @staticmethod
    def program_name() -> str:
        return "scaler Symphony worker adapter"
