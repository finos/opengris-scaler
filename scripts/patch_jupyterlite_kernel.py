#!/usr/bin/env python3
"""Patch the jupyterlite-pyodide-kernel boot bundle so opengris-scaler and its
pure-Python deps are auto-installed when the kernel starts.

jupyterlite-pyodide-kernel 0.22 has no public hook for "run this Python on
kernel boot", so we splice extra ``piplite.install`` lines into the kernel's
bootstrap statement array via a string replace on a unique marker. The wheels
are staged locally by PipliteAddon (see ``docs/source/jupyter_lite_config.json``)
so the installs resolve to local URLs with no network round-trips. The patcher
is idempotent (a sentinel comment guards against re-injection) and runs from
a Sphinx ``build-finished`` hook in ``docs/source/conf.py``.
"""

from __future__ import annotations

import argparse
from pathlib import Path

# Last statement pushed onto the bootstrap array before runPythonAsync executes
# it. We splice our installs in immediately before this line.
MARKER = 's.push("import pyodide_kernel")'
SENTINEL = "/* opengris-scaler-bootstrap-patched */"

# Packages to install at kernel boot. Each entry is (spec, deps); ``deps=False``
# skips micropip's transitive PyPI resolution for packages whose metadata pins
# versions Pyodide cannot satisfy (e.g. parfun/pargraph depending on the real
# psutil, which we replace with a stub wheel).
#
# Pyodide-bundled deps (attrs, jsonschema, msgpack, numpy, scikit-learn,
# pyparsing) are listed explicitly so piplite loads them eagerly, before
# parfun/pargraph import them during another piplite.install transaction.
# psutil and loky are stub wheels from ``scripts/wasm_stubs/``.
PACKAGES: list[tuple[str, bool]] = [
    ("opengris-scaler", True),
    ("cloudpickle", True),
    ("tblib>=3.2.0", True),
    ("attrs", True),
    ("jsonschema", True),
    ("msgpack", True),
    ("numpy", True),
    ("scikit-learn", True),
    ("pyparsing", True),
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
        # ``reinstall=True`` is required because Pyodide preloads some of these
        # packages (e.g. tblib 3.0.0) before this bootstrap runs; without it,
        # micropip raises ValueError on the version mismatch and aborts the
        # whole atomic install transaction.
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
    # The marker may appear in several bundle variants; all need the same patch.
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
            # Skip binary blobs that happen to match *.js.
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
