import dataclasses
import socket
from typing import Optional

from scaler.config import defaults
from scaler.config.common.common import CommonConfig
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.config.types.worker import WorkerNames
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class ClusterConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, nargs="?", help="the scheduler address to connect to")
    )
    object_storage_address: Optional[ObjectStorageConfig] = dataclasses.field(
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

    common_config: CommonConfig = CommonConfig()
    worker_config: WorkerConfig = WorkerConfig()
    logging_config: LoggingConfig = LoggingConfig()

    def __post_init__(self):
        if self.worker_names.names and len(self.worker_names.names) != self.num_of_workers:
            raise ValueError(
                f"the number of worker_names ({len(self.worker_names.names)}) "
                "must match num_of_workers ({self.num_of_workers})."
            )
        if not self.worker_names.names:
            self.worker_names.names = [f"{socket.gethostname().split('.')[0]}" for _ in range(self.num_of_workers)]
