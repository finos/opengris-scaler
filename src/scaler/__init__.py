import logging
from importlib import import_module
from typing import Any

from .about import __version__

# Library-mode safety: attach a NullHandler to the "scaler" logger so importing
# scaler (e.g. `from scaler import Client`) never emits "no handler" warnings
# and never alters the host application's root logger. Daemon entry points
# replace this with real handlers via `setup_logger`.
logging.getLogger("scaler").addHandler(logging.NullHandler())

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
