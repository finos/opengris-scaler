import dataclasses
import socket
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.worker import WorkerNames
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class ClusterConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, help="the scheduler address to connect to")
    )
    object_storage_address: Optional[ObjectStorageAddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-osa",
            help=(
                "the object storage server address, e.g. tcp://localhost:2346. "
                "if not specified, uses the address provided by the scheduler"
            ),
        ),
    )
    preload: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            help='optional module init in the form "pkg.mod:func(arg1, arg2)" executed in each processor before tasks'
        ),
    )
    worker_names: WorkerNames = dataclasses.field(
        default_factory=WorkerNames,
        metadata=dict(
            short="-wn", help="a comma-separated list of worker names to replace default worker names (host names)"
        ),
    )
    num_of_workers: int = dataclasses.field(
        default=defaults.DEFAULT_NUMBER_OF_WORKER, metadata=dict(short="-n", help="the number of workers in cluster")
    )
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-wit", help="set the number of io threads for io backend per worker"),
    )
    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    no_random_worker_ids: bool = dataclasses.field(
        default=False, metadata=dict(short="-nrwi", help="disable random worker id generation")
    )

    def __post_init__(self):
        if self.worker_names.names and len(self.worker_names.names) != self.num_of_workers:
            raise ValueError(
                f"the number of worker_names ({len(self.worker_names.names)}) "
                "must match num_of_workers ({self.num_of_workers})."
            )
        if not self.worker_names.names:
            self.worker_names.names = [f"{socket.gethostname().split('.')[0]}" for _ in range(self.num_of_workers)]
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
