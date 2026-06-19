"""Minimal loky shim for the JupyterLite/Pyodide site.

Real loky imports ``multiprocessing.synchronize`` at module load, which
requires the ``_multiprocessing`` C extension that Pyodide does not ship.
pargraph's ``GraphEngine.__init__`` only calls ``get_reusable_executor``
when no explicit backend is passed; the gallery notebooks always pass a
scaler-backed backend, so the function never actually runs in browser. We
expose a stub that raises if it is ever invoked, while letting the bare
``from loky import get_reusable_executor`` import inside
``pargraph/engine/engine.py`` succeed at module load time.
"""


def get_reusable_executor(*args, **kwargs):
    raise RuntimeError(
        "loky.get_reusable_executor is unavailable under Pyodide (the "
        "_multiprocessing C extension is not built for wasm32). Pass an "
        "explicit backend (e.g. a scaler-backed Executor) to GraphEngine."
    )
