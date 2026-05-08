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


def main() -> None:
    if not WHEEL_DIR.is_dir():
        raise SystemExit(f"{WHEEL_DIR} does not exist. Run scripts/build_wasm.sh first.")

    urls = []

    # Pyodide/piplite require a PEP 427-compliant wheel filename
    # ({name}-{version}-{python}-{abi}-{platform}.whl). Use the versioned
    # wheel produced by ``scripts/build_wasm.sh``.
    scaler_wheels = sorted(WHEEL_DIR.glob("opengris_scaler-*wasm32.whl"))
    if not scaler_wheels:
        raise SystemExit(f"No opengris_scaler wasm wheel in {WHEEL_DIR}. " "Run scripts/build_wasm.sh.")
    urls.append(f"_static/wasm/{scaler_wheels[-1].name}")

    for prefix in ("cloudpickle-", "tblib-"):
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


if __name__ == "__main__":
    main()
