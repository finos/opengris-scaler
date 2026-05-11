"""Minimal psutil shim for the JupyterLite/Pyodide site.

Pyodide 0.29 does not bundle psutil and the upstream package has no
pure-Python wheel (the C extension is the implementation), so it cannot
be installed in the in-browser kernel.

Three groups of code touch psutil under wasm:

1. parfun (and a couple of its transitive deps) imports psutil at module
   load only to read ``cpu_count`` for default arguments.
2. ``scaler.client.agent.heartbeat_manager`` calls ``psutil.Process()``
   at construction and then ``cpu_percent`` / ``memory_info`` every
   heartbeat. This is the path that actually runs in the browser.
3. Worker-side modules (``scaler.worker.agent.profiling_manager``,
   ``scaler.worker.agent.heartbeat_manager``,
   ``scaler.worker_manager_adapter.heartbeat_manager``) import psutil
   unconditionally and reference a few exception classes / status
   constants. They never execute in the browser, but transitive imports
   may pull them in, so the names need to exist on the module.

This shim provides just enough of that surface for all three cases:
``Process`` reports zero CPU/RSS, ``virtual_memory`` reports zero free
RAM, exception classes and status constants are present, and any other
API access raises ``AttributeError`` so it is obvious if a notebook
accidentally relies on a metric that cannot be measured in wasm.

The shim advertises the latest upstream psutil version so the version
pin parfun puts on its psutil dep (``psutil>=7.0.0``) is satisfied.
"""

from __future__ import annotations

from typing import Any, NamedTuple, Optional

# Status constants referenced by scaler worker-side modules. Kept here so
# module-level attribute lookups succeed even though no browser code path
# actually compares against them.
STATUS_RUNNING = "running"
STATUS_SLEEPING = "sleeping"
STATUS_DEAD = "dead"
STATUS_ZOMBIE = "zombie"


class Error(Exception):
    """Base psutil exception."""


class NoSuchProcess(Error):
    pass


class AccessDenied(Error):
    pass


class ZombieProcess(NoSuchProcess):
    pass


class _MemoryInfo(NamedTuple):
    rss: int = 0
    vms: int = 0


class _CPUTimes(NamedTuple):
    user: float = 0.0
    system: float = 0.0
    children_user: float = 0.0
    children_system: float = 0.0


class _VirtualMemory(NamedTuple):
    total: int = 0
    available: int = 0
    used: int = 0
    free: int = 0
    percent: float = 0.0


class Process:
    """Stub ``psutil.Process`` returning zeroed metrics.

    The browser sandbox does not expose process-level metrics, so
    ``cpu_percent`` and ``memory_info`` return zero. ``status`` always
    returns ``STATUS_RUNNING`` because the calling Python interpreter is
    by definition still running when it asks the question.
    """

    def __init__(self, pid: Optional[int] = None) -> None:
        self.pid: int = pid if pid is not None else 0

    def cpu_percent(self, *_args: Any, **_kwargs: Any) -> float:
        return 0.0

    def cpu_times(self) -> _CPUTimes:
        return _CPUTimes()

    def memory_info(self) -> _MemoryInfo:
        return _MemoryInfo()

    def status(self) -> str:
        return STATUS_RUNNING


def cpu_count(logical: bool = True) -> int:
    return 1


def virtual_memory() -> _VirtualMemory:
    return _VirtualMemory()
