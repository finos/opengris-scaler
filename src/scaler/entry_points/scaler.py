import dataclasses
import multiprocessing
import sys
from typing import List, Optional, Union

from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.section.webgui import WebGUIConfig
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger

WorkerManagerUnion = Union[
    NativeWorkerManagerConfig, SymphonyWorkerManagerConfig, ECSWorkerManagerConfig, AWSBatchWorkerManagerConfig
]


@dataclasses.dataclass
class ScalerAllConfig(ConfigClass):
    config: str = dataclasses.field(metadata=dict(positional=True, help="Path to the TOML configuration file."))
    # Declaration order = startup order (scheduler before workers).
    scheduler: Optional[SchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    worker_managers: List[WorkerManagerUnion] = dataclasses.field(
        default_factory=list, metadata=dict(section="worker_manager", discriminator="type")
    )
    gui: Optional[WebGUIConfig] = dataclasses.field(default=None, metadata=dict(section="gui"))


# Module-level functions required for multiprocessing spawn compatibility.


def _run_scheduler(config: SchedulerConfig) -> None:
    from scaler.entry_points.scheduler import main as _main

    _main(config)


def _run_worker_manager(config: WorkerManagerUnion) -> None:
    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)
    register_event_loop(config.worker_config.event_loop)
    if isinstance(config, NativeWorkerManagerConfig):
        from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

        NativeWorkerManager(config).run()
    elif isinstance(config, SymphonyWorkerManagerConfig):
        from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(config).run()
    elif isinstance(config, ECSWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

        ECSWorkerManager(config).run()
    elif isinstance(config, AWSBatchWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_hpc.worker_manager import AWSHPCWorkerManager

        AWSHPCWorkerManager(config).run()


def _run_gui(config: WebGUIConfig) -> None:
    from scaler.entry_points.webgui import main as _main

    _main(config)


def main() -> None:
    config = ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)

    if config.scheduler is None and not config.worker_managers and config.gui is None:
        print("scaler: no any recognized section found in config file", file=sys.stderr)
        sys.exit(1)

    processes: List[multiprocessing.Process] = []

    if config.scheduler is not None:
        sched_process = multiprocessing.Process(target=_run_scheduler, args=(config.scheduler,), name="scheduler")
        sched_process.start()
        processes.append(sched_process)

    for wm_config in config.worker_managers:
        wm_process = multiprocessing.Process(target=_run_worker_manager, args=(wm_config,), name=wm_config._tag)
        wm_process.start()
        processes.append(wm_process)

    if config.gui is not None:
        gui_process = multiprocessing.Process(target=_run_gui, args=(config.gui,), name="gui")
        gui_process.start()
        processes.append(gui_process)

    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
