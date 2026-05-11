"""Minimal psutil shim for the JupyterLite/Pyodide site.

Pyodide 0.29 does not bundle psutil and the upstream package has no
pure-Python wheel (it ships C source only), so it cannot be installed in
the in-browser kernel. parfun and a couple of its transitive deps import
psutil at module load time only to read ``cpu_count`` for default
arguments. The browser kernel only sees a single logical CPU anyway, so
returning 1 here is both correct and lets ``import parfun`` succeed.

If a notebook actually calls another psutil API at runtime it will raise
AttributeError, which is the correct behaviour: the real psutil cannot
run in wasm32 and silently faking arbitrary metrics would be misleading.
"""


def cpu_count(logical: bool = True) -> int:
    return 1
