import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig


@dataclasses.dataclass
class WorkerAdapterConfig(ConfigClass):
    scheduler_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler address to connect workers to")
    )

    object_storage_address: Optional[ObjectStorageAddressConfig] = dataclasses.field(
        default=None,
        metadata=dict(short="-osa", help="specify the object storage server address, e.g.: tcp://localhost:2346"),
    )

    max_workers: int = dataclasses.field(
        default=defaults.DEFAULT_NUMBER_OF_WORKER,
        metadata=dict(
            short="-mw",
            help=(
                "maximum number of workers that can be started, -1 means no limit."
                "for fixed native worker adapter, this is exactly the number of workers that will be spawned"
            ),
        ),
    )

    def __post_init__(self) -> None:
        if self.max_workers != -1 and self.max_workers < 0:
            raise ValueError("max_workers must be -1 (no limit) or a non-negative integer.")
