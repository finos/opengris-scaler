import dataclasses

from scaler.config.config_class import ConfigClass
from scaler.config.types.object_storage_server import ObjectStorageConfig

try:
    from typing import override  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import override  # type: ignore[attr-defined]


@dataclasses.dataclass
class ObjectStorageServerConfig(ConfigClass):
    object_storage_address: ObjectStorageConfig = dataclasses.field(
        metadata=dict(
            positional=True,
            nargs="?",
            help="specify the object storage server address to listen to, e.g. tcp://localhost:2345.",
        )
    )

    @override
    @staticmethod
    def section_name() -> str:
        return "object_storage_server"

    @override
    @staticmethod
    def program_name() -> str:
        return "scaler_object_storage_server"
