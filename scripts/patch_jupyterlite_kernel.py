#!/usr/bin/env python3
"""Patch the jupyterlite-pyodide-kernel boot bundle so opengris-scaler and its
pure-Python deps are auto-installed when the kernel starts.

This is what makes ``import scaler`` Just Work in the published JupyterLite
site without the user (or the notebook) having to call ``piplite.install``.

Why a JS patch?
---------------

jupyterlite-pyodide-kernel 0.22 has no public hook for "run this Python on
kernel boot". The kernel's ``initKernel`` function (in the worker bundle) builds
a list of bootstrap statements -- ``await piplite.install('ipykernel', ...)``
and friends -- then runs them via ``runPythonAsync``.

We splice three extra ``piplite.install`` lines into that list, immediately
before the final ``import pyodide_kernel`` line, by string-replacing on a
unique marker in the minified bundle. The wheels they install are already
staged into the lite output by ``PipliteAddon`` (see
``docs/source/jupyter_lite_config.json``), so ``piplite.install`` resolves to
local URLs with no network round-trips.

The patcher walks the lite output tree, finds every JS file that contains the
marker, and rewrites it once. Re-running the patcher is a no-op (it detects
the injected sentinel and skips). Run after ``jupyter lite build`` -- this
script is invoked from a Sphinx ``build-finished`` hook in ``docs/source/conf.py``.
"""

from __future__ import annotations

import argparse
from pathlib import Path

# Marker string in the kernel worker bundle. ``s`` is the array of bootstrap
# Python statements; ``s.push("import pyodide_kernel")`` is the last entry
# pushed before runPythonAsync executes the lot. We splice our installs in
# before that line so they run as part of the same boot transaction.
MARKER = 's.push("import pyodide_kernel")'

# Sentinel we leave behind so the patcher is idempotent.
SENTINEL = "/* opengris-scaler-bootstrap-patched */"

# Packages to install at kernel boot. Each entry is (spec, deps) where
# ``deps=False`` tells micropip to skip transitive PyPI dependency resolution
# for that package -- needed when a dep pins a version Pyodide cannot satisfy
# from its bundled package set or when the dep does not exist for wasm at
# all. JupyterLite's piplite resolves package names against (a) the local
# piplite_urls index built by PipliteAddon from the wheels in
# docs/source/_static/wasm/ and (b) Pyodide's own pyodide-lock.json. We list
# Pyodide-bundled deps explicitly here so the resolution happens up-front,
# before parfun/pargraph import them at module load.
#
#   - opengris-scaler: the wasm wheel itself (vendored).
#   - cloudpickle: vendored (Pyodide bundles 2.x; gallery code wants 3.x).
#   - tblib >= 3.2.0: vendored (Pyodide bundles 3.0.0 but the native worker
#     pickles exceptions via 'unpickle_exception_with_attrs', new in 3.2.0).
#   - attrs, jsonschema, msgpack, scikit-learn: Pyodide-bundled. Installed
#     explicitly so they are loaded before parfun/pargraph import them at
#     module load (the on-demand auto-loader does not always fire in time
#     for imports that happen during another piplite.install transaction).
#   - bidict, pydot: vendored pure-Python deps not in Pyodide's bundle.
#   - psutil, loky: vendored stub wheels built from scripts/wasm_stubs/.
#     Real psutil has no pure-Python wheel and real loky imports the
#     missing _multiprocessing C extension at load. The stubs satisfy the
#     module-load imports parfun/pargraph perform; runtime calls into
#     either stub raise a clear error message.
#   - opengris-parfun, pargraph: the libraries gallery notebooks import.
#     Installed with deps=False because their PyPI metadata pins
#     ``psutil>=7.0.0`` which our stub satisfies via the local index, but
#     micropip would otherwise try (and fail) to fetch a real pure-Python
#     psutil wheel from PyPI.
PACKAGES: list[tuple[str, bool]] = [
    ("opengris-scaler", True),
    ("cloudpickle", True),
    ("tblib>=3.2.0", True),
    # Pyodide-bundled deps. Listed explicitly so piplite resolves and loads
    # them before parfun/pargraph import them at module load.
    ("attrs", True),
    ("jsonschema", True),
    ("msgpack", True),
    ("numpy", True),
    ("scikit-learn", True),
    ("pyparsing", True),  # transitive dep of pydot
    ("bidict", False),
    ("pydot", False),
    ("psutil", False),
    ("loky", False),
    ("opengris-parfun", False),
    ("pargraph", False),
]


def _injection_for(packages: list[tuple[str, bool]]) -> str:
    """Build the JS that pushes our piplite.install lines onto ``s``."""
    pushes = []
    for pkg, deps in packages:
        # Each line becomes a Python statement inside the bootstrap.
        # ``reinstall=True`` is required because Pyodide preloads tblib 3.0.0
        # into the kernel before this bootstrap runs; without it, micropip
        # raises ValueError on the version mismatch and aborts the whole
        # transaction (which would also stop scaler/cloudpickle from
        # installing, since piplite.install processes the list atomically).
        deps_kw = "" if deps else ", deps=False"
        pushes.append(f"s.push(\"await piplite.install('{pkg}', keep_going=True, reinstall=True{deps_kw})\")")
    return SENTINEL + ";" + ";".join(pushes) + ";" + MARKER


def patch_file(path: Path) -> bool:
    """Patch a single bundle. Returns True iff the file was modified."""
    text = path.read_text(encoding="utf-8")
    if SENTINEL in text:
        return False
    if MARKER not in text:
        return False
    # The marker may appear multiple times (the kernel is bundled into several
    # worker variants). They are all identical and all need the same patch.
    patched = text.replace(MARKER, _injection_for(PACKAGES))
    path.write_text(patched, encoding="utf-8")
    return True


def patch_tree(root: Path) -> int:
    """Patch every .js file under ``root`` that carries the marker."""
    if not root.is_dir():
        raise FileNotFoundError(f"lite output dir not found: {root}")

    modified = 0
    for js in root.rglob("*.js"):
        try:
            if patch_file(js):
                modified += 1
                print(f"  patched {js.relative_to(root)}")
        except (OSError, UnicodeDecodeError):
            # Source maps and license files end in .js.map / .js.LICENSE.txt
            # so they don't match rglob("*.js"); anything else that fails to
            # decode is binary noise we can skip.
            continue
    return modified


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("lite_dir", type=Path, help="Path to the built JupyterLite output (e.g. docs/build/html/lite)")
    args = parser.parse_args()

    n = patch_tree(args.lite_dir)
    if n == 0:
        print(f"No bundles needed patching under {args.lite_dir}")
    else:
        print(f"Patched {n} bundle(s) under {args.lite_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
