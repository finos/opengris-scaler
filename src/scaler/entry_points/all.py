import dataclasses
import multiprocessing
import queue
import sys
from typing import List, Optional, Union

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.object_storage_server import ObjectStorageServerConfig
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig, ZMQType
from scaler.utility.event_loop import register_event_loop
from scaler.utility.logging.utility import setup_logger

WorkerManagerUnion = Union[
    NativeWorkerManagerConfig, SymphonyWorkerManagerConfig, ECSWorkerManagerConfig, AWSBatchWorkerManagerConfig
]


@dataclasses.dataclass
class ScalerAllConfig(ConfigClass):
    # Declaration order = startup order (scheduler before workers).
    logging: LoggingConfig = dataclasses.field(default_factory=LoggingConfig, metadata=dict(section="logging"))
    worker: WorkerConfig = dataclasses.field(default_factory=WorkerConfig, metadata=dict(section="worker"))
    scheduler: Optional[SchedulerConfig] = dataclasses.field(default=None, metadata=dict(section="scheduler"))
    object_storage_server: Optional[ObjectStorageServerConfig] = dataclasses.field(
        default=None, metadata=dict(section="object_storage_server")
    )
    worker_managers: List[WorkerManagerUnion] = dataclasses.field(
        default_factory=list, metadata=dict(section="worker_manager", discriminator="type")
    )


# Module-level functions required for multiprocessing spawn compatibility.


def _run_scheduler(
    config: SchedulerConfig, address_queue: Optional[multiprocessing.Queue] = None, spawn_object_storage: bool = False
) -> None:
    from scaler.entry_points.scheduler import main as _main

    _main(config, address_queue, spawn_object_storage)


def _run_worker_manager(logging: LoggingConfig, config: WorkerManagerUnion, worker_config: WorkerConfig) -> None:
    setup_logger(logging.paths, logging.config_file, logging.level)
    register_event_loop(worker_config.event_loop)
    if isinstance(config, NativeWorkerManagerConfig):
        from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

        NativeWorkerManager(config, worker_config, logging).run()
    elif isinstance(config, SymphonyWorkerManagerConfig):
        from scaler.worker_manager_adapter.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(config, worker_config).run()
    elif isinstance(config, ECSWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

        ECSWorkerManager(config, worker_config).run()
    elif isinstance(config, AWSBatchWorkerManagerConfig):
        from scaler.worker_manager_adapter.aws_hpc.worker_manager import AWSHPCWorkerManager

        AWSHPCWorkerManager(config, worker_config).run()


def _apply_actual_addresses(
    config: "ScalerAllConfig",
    actual_sched_addr: Optional[ZMQConfig],
    actual_os_addr: Optional[ObjectStorageAddressConfig],
) -> None:
    """Propagate actual bound addresses to worker managers after processes have started.

    For each worker manager:
    - scheduler_address is None  → connect on 127.0.0.1 at the actual bound port
    - scheduler_address port = 0 → keep configured host, fill in actual bound port
    - object_storage_address is None (when object storage server is running) → 127.0.0.1 at actual port
    - object_storage_address port = 0 (when object storage server is running) → keep host, fill in port
    """
    for wm_config in config.worker_managers:
        sched_addr = wm_config.worker_manager_config.scheduler_address
        if sched_addr is None or sched_addr.port == 0:
            if actual_sched_addr is None:
                print(
                    "scaler_all: worker_manager is missing scheduler_address and no [scheduler] section is defined",
                    file=sys.stderr,
                )
                sys.exit(1)
            if sched_addr is None:
                wm_config.worker_manager_config.scheduler_address = dataclasses.replace(
                    actual_sched_addr, host="127.0.0.1"
                )
            else:
                wm_config.worker_manager_config.scheduler_address = dataclasses.replace(
                    sched_addr, port=actual_sched_addr.port
                )

        os_addr = wm_config.worker_manager_config.object_storage_address
        if actual_os_addr is not None and (os_addr is None or os_addr.port == 0):
            if os_addr is None:
                wm_config.worker_manager_config.object_storage_address = dataclasses.replace(
                    actual_os_addr, host="127.0.0.1"
                )
            else:
                wm_config.worker_manager_config.object_storage_address = dataclasses.replace(
                    os_addr, port=actual_os_addr.port
                )


def _resolve_addresses(config: "ScalerAllConfig") -> None:
    """Fill in scheduler_address where omitted, inferring from the [scheduler] section."""
    if config.scheduler is not None and config.scheduler.scheduler_address is None:
        config.scheduler.scheduler_address = ZMQConfig(ZMQType.tcp, "0.0.0.0", 0)

    scheduler_address = config.scheduler.scheduler_address if config.scheduler is not None else None

    for wm_config in config.worker_managers:
        if wm_config.worker_manager_config.scheduler_address is None:
            if scheduler_address is None:
                print(
                    "scaler_all: worker_manager is missing scheduler_address and no [scheduler] section is defined",
                    file=sys.stderr,
                )
                sys.exit(1)
            # Only propagate when the address is a real (non-zero) port; otherwise main() propagates after
            # receiving the actual bound address from the scheduler process.
            if scheduler_address.port != 0:
                wm_config.worker_manager_config.scheduler_address = scheduler_address


def main() -> None:
    config = ScalerAllConfig.parse("scaler_all", "all")

    _resolve_addresses(config)

    if config.scheduler is None and config.object_storage_server is None and not config.worker_managers:
        print("scaler_all: no recognized sections found in config file", file=sys.stderr)
        sys.exit(1)

    processes: List[multiprocessing.Process] = []
    actual_os_addr: Optional[ObjectStorageAddressConfig] = None
    actual_sched_addr: Optional[ZMQConfig] = None

    if config.object_storage_server is not None:
        os_queue: multiprocessing.Queue = multiprocessing.Queue()
        os_process = ObjectStorageServerProcess(
            object_storage_address=config.object_storage_server.object_storage_address,
            logging_paths=config.logging.paths,
            logging_level=config.logging.level,
            logging_config_file=config.logging.config_file,
            address_queue=os_queue,
        )
        os_process.start()
        processes.append(os_process)

        try:
            actual_os_addr = os_queue.get(timeout=60)
        except queue.Empty:
            print("scaler_all: object storage server failed to signal its address within 60 seconds", file=sys.stderr)
            sys.exit(1)

        os_process.wait_until_ready(actual_os_addr)

    if config.scheduler is not None:
        if actual_os_addr is not None:
            config.scheduler.object_storage_address = actual_os_addr
        sched_queue: multiprocessing.Queue = multiprocessing.Queue()
        sched_process = multiprocessing.Process(
            target=_run_scheduler, args=(config.scheduler, sched_queue), name="scheduler"
        )
        sched_process.start()
        processes.append(sched_process)

        try:
            actual_sched_addr = sched_queue.get(timeout=60)
        except queue.Empty:
            print("scaler_all: scheduler failed to signal its address within 60 seconds", file=sys.stderr)
            sys.exit(1)

    _apply_actual_addresses(config, actual_sched_addr, actual_os_addr)

    for wm_config in config.worker_managers:
        wm_process = multiprocessing.Process(
            target=_run_worker_manager, args=(config.logging, wm_config, config.worker), name=wm_config._tag
        )
        wm_process.start()
        processes.append(wm_process)

    for process in processes:
        process.join()


if __name__ == "__main__":
    main()
