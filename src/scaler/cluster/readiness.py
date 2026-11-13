import multiprocessing.connection
import sys
from multiprocessing.process import BaseProcess
from typing import Any

from scaler.config.constants import READINESS_TIMEOUT_SECONDS
from scaler.config.types.address import AddressConfig

# multiprocessing.Pipe returns a PipeConnection on Windows and a Connection elsewhere
if sys.platform == "win32":
    from multiprocessing.connection import PipeConnection as ReadyReceiver
else:
    from multiprocessing.connection import Connection as ReadyReceiver


def wait_for_ready_report(
    ready_receiver: ReadyReceiver, process: BaseProcess, name: str, bind_address: AddressConfig
) -> Any:
    """Blocks until `process` sends what it bound through `ready_receiver`, or raises if it exits or times out."""
    # the sentinel wakes the wait if the process exits first, without polling its exit code
    ready = multiprocessing.connection.wait([ready_receiver, process.sentinel], READINESS_TIMEOUT_SECONDS)
    if ready_receiver in ready:
        return ready_receiver.recv()

    if process.sentinel in ready:
        process.join()
        raise RuntimeError(f"{name} exited with code {process.exitcode} before listening to {bind_address!r}")

    raise TimeoutError(f"{name} at {bind_address!r} failed to start within {READINESS_TIMEOUT_SECONDS} seconds")
