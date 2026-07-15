#!/usr/bin/env python3
"""Regenerate ``docs/source/jupyter_lite_config.json`` from the wheels in
``docs/source/_static/wasm/``.

Run from the workspace root:

    python scripts/generate_jupyterlite_config.py

jupyterlite-sphinx feeds this config to ``jupyter lite build`` during
``make html``. ``PipliteAddon.piplite_urls`` makes the listed wheels available
to ``piplite.install(...)`` from the in-browser kernel without any network
fetch -- the gallery notebooks rely on this so the only setup the user has to
do is edit the ``SCHEDULER_ADDRESS`` cell.

Paths in the config are relative to ``docs/source/`` (the directory containing
``conf.py`` and ``jupyter_lite_config.json``).
"""

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WHEEL_DIR = REPO_ROOT / "docs" / "source" / "_static" / "wasm"
CONFIG_PATH = REPO_ROOT / "docs" / "source" / "jupyter_lite_config.json"
LAUNCHPAD_MANIFEST_PATH = WHEEL_DIR / "launchpad_wheels.json"

# Pyodide built-in packages pre-loaded via pyodide.loadPackage() (faster than micropip).
# Must match what patch_jupyterlite_kernel.py installs in the JupyterLite kernel.
PYODIDE_PACKAGES = ["numpy", "scikit-learn", "micropip"]


def main() -> None:
    if not WHEEL_DIR.is_dir():
        raise SystemExit(f"{WHEEL_DIR} does not exist. Run scripts/build_wasm.sh first.")

    urls = []

    # PEP 783: pyodide-build emits the PyEmscripten platform tag
    # ``pyemscripten_<year>_<patch>_wasm32`` (e.g. pyemscripten_2026_0_wasm32 for
    # Pyodide 314 / CPython 3.14). micropip in Pyodide >= 0.29.4 installs wheels
    # carrying this tag directly, so no retagging is needed. Match the scaler
    # wasm wheel by its ``wasm32`` platform suffix so the config tracks whatever
    # ABI ``scripts/build_wasm.sh`` produced without hard-coding the tag.
    scaler_wheels = sorted(WHEEL_DIR.glob("opengris_scaler-*_wasm32.whl"))
    if not scaler_wheels:
        raise SystemExit(f"No opengris_scaler *_wasm32 wheel in {WHEEL_DIR}. " "Run scripts/build_wasm.sh.")
    urls.append(f"_static/wasm/{scaler_wheels[-1].name}")

    # Wheels in this directory fall into two groups: real PyPI wheels
    # (cloudpickle, tblib, opengris-parfun, pargraph, bidict, pydot) and
    # locally-built stub wheels for psutil/loky (see scripts/wasm_stubs/).
    # All of them must be listed in piplite_urls so the JupyterLite kernel
    # can resolve them by name without going to PyPI.
    for prefix in ("cloudpickle-", "tblib-", "opengris_parfun-", "pargraph-", "bidict-", "pydot-", "psutil-", "loky-"):
        matches = sorted(WHEEL_DIR.glob(f"{prefix}*.whl"))
        if not matches:
            raise SystemExit(
                f"No wheel matching {prefix}*.whl in {WHEEL_DIR}. " "Run scripts/build_wasm.sh to vendor it."
            )
        urls.append(f"_static/wasm/{matches[-1].name}")

    config = {"PipliteAddon": {"piplite_urls": urls}}
    CONFIG_PATH.write_text(json.dumps(config, indent=4) + "\n")
    print(f"Wrote {CONFIG_PATH.relative_to(REPO_ROOT)} with {len(urls)} wheels:")
    for url in urls:
        print(f"  - {url}")

    basenames = [u.split("/")[-1] for u in urls]
    manifest = {"pyodide_packages": PYODIDE_PACKAGES, "local_wheels": basenames}
    LAUNCHPAD_MANIFEST_PATH.write_text(json.dumps(manifest, indent=4) + "\n")
    print(f"Wrote {LAUNCHPAD_MANIFEST_PATH.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
