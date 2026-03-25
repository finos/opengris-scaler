import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger


@dataclasses.dataclass
class WorkerManagerConfig(ConfigClass):
    baremetal_native: Optional[NativeWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="worker_manager_baremetal_native")
    )
    symphony: Optional[SymphonyWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="worker_manager_symphony")
    )
    aws_raw_ecs: Optional[ECSWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="worker_manager_aws_raw_ecs")
    )
    aws_hpc: Optional[AWSBatchWorkerManagerConfig] = dataclasses.field(
        default=None, metadata=dict(subcommand="worker_manager_aws_hpc")
    )


def main() -> None:
    config = WorkerManagerConfig.parse("scaler_worker_manager", "")

    if config.baremetal_native is not None:
        from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

        setup_logger(
            config.baremetal_native.logging_config.paths,
            config.baremetal_native.logging_config.config_file,
            config.baremetal_native.logging_config.level,
        )
        register_event_loop(config.baremetal_native.worker_config.event_loop)
        NativeWorkerManager(config.baremetal_native).run()
    elif config.symphony is not None:
        from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager

        setup_logger(
            config.symphony.logging_config.paths,
            config.symphony.logging_config.config_file,
            config.symphony.logging_config.level,
        )
        register_event_loop(config.symphony.worker_config.event_loop)
        SymphonyWorkerManager(config.symphony).run()
    elif config.aws_raw_ecs is not None:
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

        setup_logger(
            config.aws_raw_ecs.logging_config.paths,
            config.aws_raw_ecs.logging_config.config_file,
            config.aws_raw_ecs.logging_config.level,
        )
        register_event_loop(config.aws_raw_ecs.worker_config.event_loop)
        ECSWorkerManager(config.aws_raw_ecs).run()
    elif config.aws_hpc is not None:
        from scaler.worker_manager_adapter.aws_hpc.worker_manager import AWSHPCWorkerManager

        setup_logger(
            config.aws_hpc.logging_config.paths,
            config.aws_hpc.logging_config.config_file,
            config.aws_hpc.logging_config.level,
        )
        register_event_loop(config.aws_hpc.worker_config.event_loop)
        AWSHPCWorkerManager(config.aws_hpc).run()


if __name__ == "__main__":
    main()
