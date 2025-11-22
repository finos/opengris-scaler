import dataclasses

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.worker import WorkerCapabilities


@dataclasses.dataclass
class WorkerConfig(ConfigClass):
    per_worker_capabilities: WorkerCapabilities = dataclasses.field(
        default_factory=WorkerCapabilities,
        metadata=dict(
            short="-pwc", help='a comma-separated list of capabilities provided by the workers (e.g. "linux,cpu=4")'
        ),
    )
    per_worker_task_queue_size: int = dataclasses.field(
        default=defaults.DEFAULT_PER_WORKER_QUEUE_SIZE,
        metadata=dict(short="-wtqs", help="set the per worker queue size"),
    )
    heartbeat_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        metadata=dict(short="-his", help="the interval at which to send heartbeats in seconds"),
    )
    task_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_TASK_TIMEOUT_SECONDS,
        metadata=dict(
            short="-tts", help="the number of seconds before a task is considered timed out and an error is raised"
        ),
    )
    death_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT, metadata=dict(short="-ds", help="death timeout seconds")
    )
    garbage_collect_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        metadata=dict(short="-gc", help="the interval at which the garbage collector is run in seconds"),
    )
    trim_memory_threshold_bytes: int = dataclasses.field(
        default=defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        metadata=dict(short="-tm", help="set the threshold for trimming libc's memory"),
    )
    hard_processor_suspend: bool = dataclasses.field(
        default=defaults.DEFAULT_HARD_PROCESSOR_SUSPEND,
        metadata=dict(
            short="-hps",
            action="store_true",
            help=(
                "when set, suspends worker processors using the SIGTSTP signal instead of a synchronization event, "
                "fully halting computation on suspended tasks. this may cause some tasks to fail if they "
                "do not support being paused at the OS level (e.g. tasks requiring active network connections)"
            ),
        ),
    )

    def __post_init__(self) -> None:
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
