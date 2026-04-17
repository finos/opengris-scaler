# PYTHON_ARGCOMPLETE_OK
import dataclasses
import multiprocessing
import multiprocessing.connection
import sys
from typing import List, Optional, cast

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.object_storage_server import ObjectStorageServerConfig
from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.section.webgui import WebGUIConfig
from scaler.config.section.worker_manager_union import WorkerManagerUnion
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger


@dataclasses.dataclass
class ScalerAllConfig(ConfigClass):
    config: str = dataclasses.field(metadata=dict(positional=True, help="Path to the TOML configuration file."))
    # Declaration order = startup order (object storage before scheduler, scheduler before workers).
    object_storage: Optional[ObjectStorageServerConfig] = dataclasses.field(
        default=None, metadata=dict(section="object_storage_server")
    )
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
    elif isinstance(config, ORBAWSEC2WorkerAdapterConfig):
        from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerAdapter

        ORBAWSEC2WorkerAdapter(config).run()


def _run_gui(config: WebGUIConfig) -> None:
    from scaler.entry_points.webgui import main as _main

    _main(config)


def main() -> None:
    config = ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)

    if config.object_storage is None and config.scheduler is None and not config.worker_managers and config.gui is None:
        print("scaler: no any recognized section found in config file", file=sys.stderr)
        sys.exit(1)

    _spawn_process = multiprocessing.get_context("spawn").Process
    processes: List[multiprocessing.Process] = []

    if config.object_storage is not None:
        oss_logging = config.object_storage.logging_config
        oss_process = ObjectStorageServerProcess(
            bind_address=config.object_storage.bind_address,
            identity=config.object_storage.identity,
            logging_paths=oss_logging.paths,
            logging_config_file=oss_logging.config_file,
            logging_level=oss_logging.level,
        )
        oss_process.start()
        oss_process.wait_until_ready()
        processes.append(oss_process)

    if config.scheduler is not None:
        sched_process = _spawn_process(target=_run_scheduler, args=(config.scheduler,), name="scheduler")
        sched_process.start()
        processes.append(sched_process)  # type: ignore[arg-type]

    for wm_config in config.worker_managers:
        wm_process = _spawn_process(target=_run_worker_manager, args=(wm_config,), name=wm_config._tag)
        wm_process.start()
        processes.append(wm_process)  # type: ignore[arg-type]

    if config.gui is not None:
        gui_process = _spawn_process(target=_run_gui, args=(config.gui,), name="gui")
        gui_process.start()
        processes.append(gui_process)  # type: ignore[arg-type]

    try:
        sentinel_to_process = {p.sentinel: p for p in processes}
        done = multiprocessing.connection.wait(sentinel_to_process)
        exited = sentinel_to_process[cast(int, done[0])]
        for other in processes:
            other.terminate()
        for other in processes:
            other.join()
        sys.exit(exited.exitcode or 0)
    except KeyboardInterrupt:
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()


if __name__ == "__main__":
    main()
