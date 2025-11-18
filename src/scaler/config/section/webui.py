import dataclasses
from typing import Optional, Tuple

from scaler.config import defaults
from scaler.config.config_class import ConfigClass
from scaler.config.types.zmq import ZMQConfig

try:
    from typing import override  # type: ignore[attr-defined]
except ImportError:
    from typing_extensions import override  # type: ignore[attr-defined]


@dataclasses.dataclass
class WebUIConfig(ConfigClass):
    monitor_address: ZMQConfig = dataclasses.field(
        metadata=dict(positional=True, help="scheduler monitor address to connect to")
    )
    web_host: str = dataclasses.field(default="0.0.0.0", metadata=dict(help="host for webserver to connect to"))
    web_port: int = dataclasses.field(default=50001, metadata=dict(help="host for webserver to connect to"))
    logging_paths: Tuple[str, ...] = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_PATHS,
        metadata=dict(
            short="-lp",
            type=str,
            nargs="*",
            help="specify where webui log should be logged to, it can accept multiple files, default is /dev/stdout",
        ),
    )
    logging_config_file: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            short="-lc",
            help="use standard python the .conf file the specify python logging file configuration format, this will "
            "bypass --logging-path",
        ),
    )
    logging_level: str = dataclasses.field(
        default=defaults.DEFAULT_LOGGING_LEVEL, metadata=dict(short="-ll", help="specify the logging level")
    )

    def __post_init__(self):
        if not isinstance(self.web_host, str):
            raise TypeError(f"Web host should be string, given {self.web_host}")
        if not isinstance(self.web_port, int):
            raise TypeError(f"Web port should be an integer, given {self.web_port}")

    @override
    @staticmethod
    def section_name() -> str:
        return "webui"

    @override
    @staticmethod
    def program_name() -> str:
        return "web ui for scaler monitoring"
