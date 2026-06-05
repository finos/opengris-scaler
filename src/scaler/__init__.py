from importlib import import_module
from typing import Any

from .about import __version__

__all__ = ["__version__", "Client", "ScalerFuture", "Serializer", "SchedulerClusterCombo", "Scheduler"]


def __getattr__(name: str) -> Any:
    if name in {"Client", "ScalerFuture"}:
        module = import_module(".client.client", __name__)
        return getattr(module, name)

    if name == "Serializer":
        module = import_module(".client.serializer.mixins", __name__)
        return getattr(module, name)

    if name == "SchedulerClusterCombo":
        module = import_module(".cluster.combo", __name__)
        return getattr(module, name)

    if name == "Scheduler":
        module = import_module(".cluster.scheduler", __name__)
        return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
