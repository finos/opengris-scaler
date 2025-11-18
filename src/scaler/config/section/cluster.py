import dataclasses
import socket
from typing import Optional, Tuple

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerCapabilities, WorkerNames
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.event_loop import EventLoopType
from scaler.utility.logging.utility import LoggingLevel

try:
    from typing import override  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import override  # type: ignore[attr-defined]


@dataclasses.dataclass
class ClusterConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, nargs="?", help="scheduler address to connect to")
    )
    object_storage_address: Optional[ObjectStorageConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-osa",
            help=(
                "specify the object storage server address, "
                "e.g. tcp://localhost:2346. If not specified, use the address provided by the scheduler"
            ),
        ),
    )
    preload: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            help='optional module init in the form "pkg.mod:func(arg1, arg2)" executed in each processor before tasks'
        ),
    )
    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS, metadata=dict(short="-wit", help="specify number of io threads per worker")
    )
    worker_names: WorkerNames = dataclasses.field(
        default_factory=lambda: WorkerNames([]),
        metadata=dict(short="-wn", help="worker names to replace default worker names (host names), separate by comma"),
    )
    num_of_workers: int = dataclasses.field(
        default=defaults.DEFAULT_NUMBER_OF_WORKER, metadata=dict(short="-n", help="number of workers in cluster")
    )
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=lambda: WorkerCapabilities({}),
        metadata=dict(
            short="-pwc", help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")'
        ),
    )
    per_worker_task_queue_size: int = dataclasses.field(
        default=defaults.DEFAULT_PER_WORKER_QUEUE_SIZE,
        metadata=dict(short="-wtqs", help="specify per worker queue size"),
    )
    heartbeat_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        metadata=dict(short="-hi", help="number of seconds to send heartbeat interval"),
    )
    task_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_TASK_TIMEOUT_SECONDS,
        metadata=dict(short="-tts", help="number of seconds task treat as timeout and return an exception"),
    )
    death_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT, metadata=dict(short="-ds", help="death timeout seconds")
    )
    garbage_collect_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        metadata=dict(short="-gc", help="garbage collect interval seconds"),
    )
    trim_memory_threshold_bytes: int = dataclasses.field(
        default=defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        metadata=dict(short="-tm", help="number of bytes threshold to enable libc to trim memory"),
    )
    hard_processor_suspend: bool = dataclasses.field(
        default=defaults.DEFAULT_HARD_PROCESSOR_SUSPEND,
        metadata=dict(
            short="-hps",
            action="store_true",
            help=(
                "When set, suspends worker processors using the SIGTSTP signal instead of a synchronization event, "
                "fully halting computation on suspended tasks. Note that this may cause some tasks to fail if they "
                "do not support being paused at the OS level (e.g. tasks requiring active network connections)."
            ),
        ),
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select event loop type"),
    )
    logging_paths: Tuple[str, ...] = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_PATHS,
        metadata=dict(
            type=str,
            short="-lp",
            nargs="*",
            help='specify where cluster log should logged to, it can be multiple paths, "/dev/stdout" is default for '
            "standard output, each worker will have its own log file with process id appended to the path",
        ),
    )
    logging_config_file: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-lcf",
            help="use standard python the .conf file the specify python logging file configuration format, this will "
            "bypass --logging-paths and --logging-level at the same time, and this will not work on per worker logging",
        ),
    )
    logging_level: str = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_LEVEL,
        metadata=dict(
            short="-ll", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), help="specify the logging level"
        ),
    )

    def __post_init__(self):
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
        if self.worker_names.names and len(self.worker_names.names) != self.num_of_workers:
            raise ValueError(
                f"The number of worker_names ({len(self.worker_names.names)}) "
                    "must match num_of_workers ({self.num_of_workers})."
            )
        if not self.worker_names.names:
            self.worker_names.names = [f"{socket.gethostname().split('.')[0]}" for _ in range(self.num_of_workers)]
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

    @override
    @staticmethod
    def section_name() -> str:
        return "cluster"

    @override
    @staticmethod
    def program_name() -> str:
        return "cluster"
