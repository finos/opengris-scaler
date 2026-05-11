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

# Packages to install at kernel boot. Order matches what the gallery notebooks
# need:
#   - opengris-scaler: the wasm wheel itself (resolved from the local
#     piplite index built by PipliteAddon).
#   - cloudpickle: not in Pyodide's bundled package set; needed by Client to
#     pickle user functions.
#   - tblib >= 3.2.0: Pyodide 0.29.x bundles tblib 3.0.0, but the native
#     worker pickles exceptions via 'unpickle_exception_with_attrs', which
#     was added in 3.2.0.
#   - opengris-parfun, pargraph: pure-Python parallel-task libraries the
#     gallery notebooks import directly.
PACKAGES = ["opengris-scaler", "cloudpickle", "tblib>=3.2.0", "opengris-parfun", "pargraph"]


def _injection_for(packages: list[str]) -> str:
    """Build the JS that pushes our piplite.install lines onto ``s``."""
    pushes = []
    for pkg in packages:
        # Each line becomes a Python statement inside the bootstrap.
        # ``reinstall=True`` is required because Pyodide preloads tblib 3.0.0
        # into the kernel before this bootstrap runs; without it, micropip
        # raises ValueError on the version mismatch and aborts the whole
        # transaction (which would also stop scaler/cloudpickle from
        # installing, since piplite.install processes the list atomically).
        pushes.append(f"s.push(\"await piplite.install('{pkg}', keep_going=True, reinstall=True)\")")
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
