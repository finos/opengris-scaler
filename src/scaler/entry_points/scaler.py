# PYTHON_ARGCOMPLETE_OK
import dataclasses
import multiprocessing
import multiprocessing.connection
import signal
import sys
import time
from multiprocessing.connection import Connection
from typing import List, Optional, Tuple, cast

import psutil

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.readiness import wait_for_ready_report
from scaler.config.config_class import ConfigClass
from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.object_storage_server import ObjectStorageServerConfig
from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.config.section.oci_raw_worker_manager import OCIRawWorkerManagerConfig
from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig
from scaler.config.section.webgui import WebGUIConfig
from scaler.config.section.worker_manager_union import WorkerManagerUnion
from scaler.config.types.address import AddressConfig
from scaler.scheduler.scheduler import BoundSchedulerAddresses
from scaler.utility.event_loop import register_event_loop
from scaler.utility.network_util import OS_PICKED_PORT, resolve_port_zero
from scaler.utility.process_bootstrap import bootstrap_process


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


def _run_scheduler(config: SchedulerConfig, ready_sender: Connection) -> None:
    from scaler.entry_points.scheduler import main as _main

    _main(config, on_ready=ready_sender.send)


def _run_worker_manager(config: WorkerManagerUnion) -> None:
    bootstrap_process(
        config.logging_config.paths,
        config.logging_config.config_file,
        config.logging_config.level,
        process_name=config._tag,
    )
    register_event_loop(config.worker_config.event_loop)
    if isinstance(config, NativeWorkerManagerConfig):
        from scaler.worker_manager.native.worker_manager import NativeWorkerManager

        NativeWorkerManager(config).run()
    elif isinstance(config, SymphonyWorkerManagerConfig):
        from scaler.worker_manager.proxy.symphony.worker_manager import SymphonyWorkerManager

        SymphonyWorkerManager(config).run()
    elif isinstance(config, ECSWorkerManagerConfig):
        from scaler.worker_manager.cloud.aws_ecs.worker_manager import ECSWorkerManager

        ECSWorkerManager(config).run()
    elif isinstance(config, AWSBatchWorkerManagerConfig):
        from scaler.worker_manager.proxy.aws_batch.worker_manager import AWSBatchWorkerManager

        AWSBatchWorkerManager(config).run()
    elif isinstance(config, ORBAWSEC2WorkerManagerConfig):
        from scaler.worker_manager.cloud.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager

        ORBAWSEC2WorkerManager(config).run()
    elif isinstance(config, OCIRawWorkerManagerConfig):
        from scaler.worker_manager.cloud.oci.worker_manager import OCIInstancesWorkerManager

        OCIInstancesWorkerManager(config).run()
    elif isinstance(config, OCIHPCWorkerManagerConfig):
        from scaler.worker_manager.proxy.oci.worker_manager import OCIJobsWorkerManager

        OCIJobsWorkerManager(config).run()


def _run_gui(config: WebGUIConfig) -> None:
    from scaler.entry_points.webgui import main as _main

    _main(config)


# binding it means every interface, but connecting to it is not portable
WILDCARD_HOST = "0.0.0.0"


def _port_zero_connecting_addresses(config: ScalerAllConfig) -> List[Tuple[str, AddressConfig, bool]]:
    """Each connecting field that names port 0, its address, and whether a section in this config binds it."""
    fields: List[Tuple[str, AddressConfig, bool]] = []
    if config.scheduler is not None:
        fields.append(
            (
                "scheduler.object_storage_address",
                config.scheduler.object_storage_address,
                config.object_storage is not None,
            )
        )

    for wm_config in config.worker_managers:
        wm_addresses = wm_config.worker_manager_config
        fields.append(
            (f"{wm_config._tag}.scheduler_address", wm_addresses.scheduler_address, config.scheduler is not None)
        )
        if wm_addresses.object_storage_address is not None:
            fields.append(
                (
                    f"{wm_config._tag}.object_storage_address",
                    wm_addresses.object_storage_address,
                    config.object_storage is not None,
                )
            )

    if config.gui is not None:
        fields.append(("gui.monitor_address", config.gui.monitor_address, config.scheduler is not None))

    return [(name, address, has_target) for name, address, has_target in fields if address.port == OS_PICKED_PORT]


def _port_zero_errors(config: ScalerAllConfig) -> List[str]:
    """Why each connecting field naming port 0 cannot be resolved, if it cannot."""
    errors = []
    for name, address, has_target in _port_zero_connecting_addresses(config):
        if not has_target:
            errors.append(f"{name} names port 0, but no section in this config binds it")
        if address.host == WILDCARD_HOST:
            errors.append(f"{name} names {WILDCARD_HOST}, which cannot be connected to: name 127.0.0.1 or another host")
    return errors


def _resolve_worker_manager(
    wm_config: WorkerManagerUnion,
    bound_scheduler_addresses: Optional[BoundSchedulerAddresses],
    object_storage_address: Optional[AddressConfig],
) -> WorkerManagerUnion:
    wm_addresses = wm_config.worker_manager_config
    if bound_scheduler_addresses is not None:
        wm_addresses = dataclasses.replace(
            wm_addresses,
            scheduler_address=resolve_port_zero(wm_addresses.scheduler_address, bound_scheduler_addresses.bind_address),
        )
    if object_storage_address is not None and wm_addresses.object_storage_address is not None:
        wm_addresses = dataclasses.replace(
            wm_addresses,
            object_storage_address=resolve_port_zero(wm_addresses.object_storage_address, object_storage_address),
        )
    return dataclasses.replace(wm_config, worker_manager_config=wm_addresses)


SHUTDOWN_JOIN_TIMEOUT_SECONDS = 10
FORCE_KILL_JOIN_TIMEOUT_SECONDS = 5


def _shutdown_processes(processes: List[multiprocessing.Process]) -> None:
    """Terminate the started child processes, then force-kill anything that did not exit in time.

    Children are terminated in reverse startup order (workers before the scheduler, the scheduler
    before object storage). Descendants are snapshotted before terminating so that grandchildren
    orphaned by a dying child (e.g. worker processors) can be reaped as well; a surviving orphan
    would keep its ports bound and also wedge interpreter exit, because the multiprocessing
    resource tracker waits for every inherited pipe handle to close.
    """

    started = [process for process in processes if process.pid is not None]

    descendants: List[psutil.Process] = []
    for process in started:
        try:
            descendants.extend(psutil.Process(process.pid).children(recursive=True))
        except psutil.NoSuchProcess:
            pass

    try:
        for process in reversed(started):
            if process.is_alive():
                process.terminate()

        deadline = time.monotonic() + SHUTDOWN_JOIN_TIMEOUT_SECONDS
        for process in reversed(started):
            process.join(max(0.0, deadline - time.monotonic()))
    except KeyboardInterrupt:
        pass  # a repeated interrupt skips the graceful wait; force-kill below

    while True:
        try:
            for process in started:
                if process.is_alive():
                    process.kill()
            for process in started:
                process.join(FORCE_KILL_JOIN_TIMEOUT_SECONDS)

            alive_descendants = []
            for descendant in descendants:
                try:
                    if descendant.is_running():
                        descendant.kill()
                        alive_descendants.append(descendant)
                except psutil.NoSuchProcess:
                    pass
            psutil.wait_procs(alive_descendants, timeout=FORCE_KILL_JOIN_TIMEOUT_SECONDS)
            return
        except KeyboardInterrupt:
            continue


def main() -> None:
    config = ScalerAllConfig.parse("scaler", "all", disable_config_flag=True)

    if config.object_storage is None and config.scheduler is None and not config.worker_managers and config.gui is None:
        print("scaler: no any recognized section found in config file", file=sys.stderr)
        sys.exit(1)

    port_zero_errors = _port_zero_errors(config)
    if port_zero_errors:
        for error in port_zero_errors:
            print(f"scaler: {error}", file=sys.stderr)
        sys.exit(1)

    def _raise_keyboard_interrupt(*_args: object) -> None:
        raise KeyboardInterrupt

    # Route SIGTERM through the same orderly-teardown path as Ctrl-C; without this, `kill <pid>`
    # ends this process silently and leaks the whole cluster, with the scheduler and object
    # storage children keeping their ports bound. On Windows SIGTERM cannot be delivered
    # externally, registering the handler is harmless there.
    signal.signal(signal.SIGTERM, _raise_keyboard_interrupt)

    _spawn_process = multiprocessing.get_context("spawn").Process
    processes: List[multiprocessing.Process] = []

    # the addresses the processes bound, which a later section naming port 0 connects to
    object_storage_address: Optional[AddressConfig] = None
    bound_scheduler_addresses: Optional[BoundSchedulerAddresses] = None

    exit_code = 0
    try:
        if config.object_storage is not None:
            oss_logging = config.object_storage.logging_config
            oss_process = ObjectStorageServerProcess(
                bind_address=config.object_storage.bind_address,
                identity=config.object_storage.identity,
                logging_paths=oss_logging.paths,
                logging_config_file=oss_logging.config_file,
                logging_level=oss_logging.level,
                security_config=config.object_storage.security,
            )
            processes.append(oss_process)
            oss_process.start()
            object_storage_address = oss_process.wait_until_ready()

        if config.scheduler is not None:
            scheduler_config = config.scheduler
            if object_storage_address is not None:
                scheduler_config = dataclasses.replace(
                    scheduler_config,
                    object_storage_address=resolve_port_zero(
                        scheduler_config.object_storage_address, object_storage_address
                    ),
                )
            ready_receiver, ready_sender = multiprocessing.get_context("spawn").Pipe(duplex=False)
            sched_process = _spawn_process(
                target=_run_scheduler, args=(scheduler_config, ready_sender), name="scheduler"
            )
            processes.append(sched_process)  # type: ignore[arg-type]
            sched_process.start()
            bound_scheduler_addresses = wait_for_ready_report(
                ready_receiver, sched_process, "Scheduler", scheduler_config.bind_address
            )

        for wm_config in config.worker_managers:
            wm_config = _resolve_worker_manager(wm_config, bound_scheduler_addresses, object_storage_address)
            wm_process = _spawn_process(target=_run_worker_manager, args=(wm_config,), name=wm_config._tag)
            processes.append(wm_process)  # type: ignore[arg-type]
            wm_process.start()

        if config.gui is not None:
            gui_config = config.gui
            if bound_scheduler_addresses is not None:
                gui_config = dataclasses.replace(
                    gui_config,
                    monitor_address=resolve_port_zero(
                        gui_config.monitor_address, bound_scheduler_addresses.monitor_address
                    ),
                )
            gui_process = _spawn_process(target=_run_gui, args=(gui_config,), name="gui")
            processes.append(gui_process)  # type: ignore[arg-type]
            gui_process.start()

        sentinel_to_process = {p.sentinel: p for p in processes}
        done = multiprocessing.connection.wait(sentinel_to_process)
        exited = sentinel_to_process[cast(int, done[0])]
        exit_code = exited.exitcode or 0
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown_processes(processes)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
