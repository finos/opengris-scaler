import dataclasses

from scaler.config.common.common import CommonConfig
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.web import WebConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class NativeWorkerAdapterConfig(ConfigClass):
    web_config: WebConfig
    worker_adapter_config: WorkerAdapterConfig
    common_config: CommonConfig = CommonConfig()
    worker_config: WorkerConfig = WorkerConfig()
    logging_config: LoggingConfig = LoggingConfig()
