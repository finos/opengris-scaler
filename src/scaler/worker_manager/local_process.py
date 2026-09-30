import asyncio
import logging
import multiprocessing.process
import time
from typing import Set

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.utility.exitcode import describe_exitcode
from scaler.utility.network_util import get_available_tcp_port

logger = logging.getLogger(__name__)

# A process check costs nothing, so a manager notices a dead local process within a second.
LOCAL_PROCESS_POLL_INTERVAL_SECONDS = 1

# A worker tears down in about 8 seconds: 5 to notify the scheduler, 3 to stop its processors.
LOCAL_PROCESS_STOP_TIMEOUT_SECONDS = 30
LOCAL_PROCESS_EXIT_POLL_SECONDS = 0.1


def local_children_address(worker_manager_config: WorkerManagerConfig) -> AddressConfig:
    """The address a manager binds for its local worker processes: the configured one, else a free loopback port.

    TCP rather than a Unix-domain socket, because Windows runs these managers too.
    """
    if worker_manager_config.children_address is not None:
        return worker_manager_config.children_address
    return AddressConfig(SocketType.tcp, "127.0.0.1", get_available_tcp_port())


def poll_local_processes(
    processes: Set[multiprocessing.process.BaseProcess],
) -> Set[multiprocessing.process.BaseProcess]:
    """Return the processes that still run, and reap the ones that exited."""
    alive = {process for process in processes if process.is_alive()}
    for process in processes - alive:
        process.join()
        exitcode = describe_exitcode(process.exitcode)
        logger.info(f"process {process.name!r} (pid={process.pid}) exited (exitcode={exitcode})")
    return alive


async def stop_local_process(process: multiprocessing.process.BaseProcess) -> None:
    """Terminate the process, kill it if it outlives the timeout, and reap it."""
    process.terminate()

    deadline = time.monotonic() + LOCAL_PROCESS_STOP_TIMEOUT_SECONDS
    while process.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(LOCAL_PROCESS_EXIT_POLL_SECONDS)

    if process.is_alive():
        logger.warning(
            f"process {process.name!r} (pid={process.pid}) outlived {LOCAL_PROCESS_STOP_TIMEOUT_SECONDS}s, killing it"
        )
        process.kill()

    process.join()
