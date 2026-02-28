import dataclasses
import enum
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


class AWSHPCBackend(enum.Enum):
    batch = enum.auto()
    # future: parallelcluster = enum.auto()
    # future: lambda_ = enum.auto()


DEFAULT_AWS_MAX_CONCURRENT_JOBS = 100
DEFAULT_AWS_JOB_TIMEOUT_MINUTES = 60
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_S3_PREFIX = "scaler-tasks"


@dataclasses.dataclass
class AWSBatchWorkerAdapterConfig(ConfigClass):
    worker_adapter_config: WorkerAdapterConfig

    job_queue: str = dataclasses.field(metadata=dict(short="-q", help="AWS Batch job queue name"))
    job_definition: str = dataclasses.field(metadata=dict(short="-d", help="AWS Batch job definition name"))
    s3_bucket: str = dataclasses.field(metadata=dict(help="S3 bucket for task data"))

    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    backend: AWSHPCBackend = dataclasses.field(
        default=AWSHPCBackend.batch, metadata=dict(short="-b", help="AWS HPC backend")
    )
    name: Optional[str] = dataclasses.field(
        default=None, metadata=dict(short="-n", help="worker name (default: aws-{backend}-worker)")
    )
    aws_region: str = dataclasses.field(default=DEFAULT_AWS_REGION, metadata=dict(help="AWS region"))
    s3_prefix: str = dataclasses.field(default=DEFAULT_S3_PREFIX, metadata=dict(help="S3 prefix for task data"))
    max_concurrent_jobs: int = dataclasses.field(
        default=DEFAULT_AWS_MAX_CONCURRENT_JOBS, metadata=dict(short="-mcj", help="maximum concurrent jobs")
    )
    job_timeout_minutes: int = dataclasses.field(
        default=DEFAULT_AWS_JOB_TIMEOUT_MINUTES, metadata=dict(help="job timeout in minutes")
    )
    heartbeat_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        metadata=dict(short="-his", help="heartbeat interval in seconds"),
    )
    death_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT, metadata=dict(short="-dts", help="death timeout in seconds")
    )
    task_queue_size: int = dataclasses.field(
        default=defaults.DEFAULT_PER_WORKER_QUEUE_SIZE, metadata=dict(help="size of the internal task queue")
    )
    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS, metadata=dict(short="-wit", help="number of IO threads for the worker")
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    def __post_init__(self) -> None:
        if not self.job_queue:
            raise ValueError("job_queue cannot be an empty string.")
        if not self.job_definition:
            raise ValueError("job_definition cannot be an empty string.")
        if not self.s3_bucket:
            raise ValueError("s3_bucket cannot be an empty string.")
        if self.max_concurrent_jobs <= 0:
            raise ValueError("max_concurrent_jobs must be a positive integer.")
        if self.job_timeout_minutes <= 0:
            raise ValueError("job_timeout_minutes must be a positive integer.")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be a positive integer.")
        if self.death_timeout_seconds <= 0:
            raise ValueError("death_timeout_seconds must be a positive integer.")
        if self.task_queue_size <= 0:
            raise ValueError("task_queue_size must be a positive integer.")
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
