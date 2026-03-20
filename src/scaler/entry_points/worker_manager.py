import dataclasses
from typing import Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger


@dataclasses.dataclass
class _WorkerManagerConfig(ConfigClass):
    logging: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    worker: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)

    baremetal_native: Optional[NativeWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="baremetal_native")
    )
    symphony: Optional[SymphonyWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="symphony")
    )
    aws_raw_ecs: Optional[ECSWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="aws_raw_ecs")
    )
    aws_hpc: Optional[AWSBatchWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="aws_hpc")
    )


def main() -> None:
    config = _WorkerManagerConfig.parse("scaler_worker_manager", "")

    setup_logger(config.logging.paths, config.logging.config_file, config.logging.level)

    register_event_loop(config.worker.event_loop)

    if config.baremetal_native is not None:
        from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

        NativeWorkerManager(config.baremetal_native, config.worker, config.logging).run()
    elif config.symphony is not None:
        from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(config.symphony, config.worker).run()
    elif config.aws_raw_ecs is not None:
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

        ECSWorkerManager(config.aws_raw_ecs, config.worker).run()
    elif config.aws_hpc is not None:
        from scaler.worker_manager_adapter.aws_hpc.worker_manager import AWSHPCWorkerManager

        AWSHPCWorkerManager(config.aws_hpc, config.worker).run()


if __name__ == "__main__":
    main()
