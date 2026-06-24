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


def _browser_unsupported_stub(name: str, reason: str) -> Any:
    """Return a placeholder class whose instantiation raises a clear error.

    Used under the JupyterLite/Pyodide kernel for symbols that depend on
    multiprocessing or native extensions that are not available in wasm32.
    The placeholder lets ``from scaler import SchedulerClusterCombo`` (which
    libraries such as parfun do at module load time when registering
    backends) succeed, while still failing loudly if anyone actually tries
    to spin up a local cluster from inside the browser.
    """

    class _BrowserUnsupported:
        def __init__(self, *_args: Any, **_kwargs: Any) -> None:
            raise RuntimeError(
                f"{name} is not available in the JupyterLite/Pyodide kernel "
                f"({reason}). Connect a Client to an external scheduler "
                f"address instead."
            )

    _BrowserUnsupported.__name__ = name
    _BrowserUnsupported.__qualname__ = name
    return _BrowserUnsupported


def __getattr__(name: str) -> Any:
    if name in {"Client", "ScalerFuture"}:
        module = import_module(".client.client", __name__)
        return getattr(module, name)

    if name == "Serializer":
        module = import_module(".client.serializer.mixins", __name__)
        return getattr(module, name)

    if name == "SchedulerClusterCombo":
        try:
            module = import_module(".cluster.combo", __name__)
        except ModuleNotFoundError as exc:
            import sys

            if sys.platform == "emscripten":
                return _browser_unsupported_stub("SchedulerClusterCombo", f"missing dependency: {exc.name}")
            raise
        return getattr(module, name)

    if name == "Scheduler":
        try:
            module = import_module(".cluster.scheduler", __name__)
        except ModuleNotFoundError as exc:
            import sys

            if sys.platform == "emscripten":
                return _browser_unsupported_stub("Scheduler", f"missing dependency: {exc.name}")
            raise
        return getattr(module, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
