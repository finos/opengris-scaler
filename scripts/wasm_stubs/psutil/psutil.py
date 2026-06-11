"""Minimal psutil shim for the JupyterLite/Pyodide site.

Pyodide 0.29 cannot install upstream psutil (C extension, no pure-Python wheel).
This shim covers the surface scaler's client/worker code touches: ``Process``
reports zero CPU/RSS, ``virtual_memory`` reports zeros, exception classes and
status constants are present. The advertised version satisfies parfun's
``psutil>=7.0.0`` pin.
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
