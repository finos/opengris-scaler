#!/usr/bin/env bash
# build_wasm.sh — build the wasm wheel and deploy it to the docs site.
#
# Run from the workspace root:
#   ./scripts/build_wasm.sh
#
# This script intentionally uses a dedicated Python 3.13 virtual environment
# under the user's cache directory instead of the project's main .venv. The
# normal dev venv stays on Python 3.12 for editing, linting, and docs builds;
# the wasm build needs the same Python version CI uses so pyodide-build
# resolves the matching xbuildenv / Emscripten toolchain.
#
# THIRD_PARTY_DIR controls where the wasm toolchain lives (emsdk, wasm-target
# capnp/libuv). Defaults to ./thirdparties; the devcontainer sets it to
# /opt/scaler via the Dockerfile ENV.
#
# CPython 3.13 cross-component constraint: the produced wheel embeds the
# CPython 3.13 ABI of bootstrap.cpp's capnp glue. The scheduler and worker(s)
# the wasm client connects to MUST also be running CPython 3.13, otherwise
# capnp struct decoding will fail with opaque errors. Only the wasm client is
# pinned to 3.13 by Pyodide; native scheduler/worker venvs need to match.

set -euo pipefail

THIRD_PARTY_DIR="${THIRD_PARTY_DIR:-${PWD}/thirdparties}"
EMSDK_ENV="${THIRD_PARTY_DIR}/emsdk/emsdk_env.sh"
WASM_INSTALL="${THIRD_PARTY_DIR}/wasm/install"
WASM_VENV_ROOT="${XDG_CACHE_HOME:-${HOME}/.cache}/opengris-scaler"
WASM_VENV="${WASM_VENV_ROOT}/pyodide-build-venv"

if ! command -v uv >/dev/null 2>&1; then
    echo "uv is required to create the Python 3.13 wasm build environment."
    exit 1
fi

# 1. Create / refresh the dedicated Python 3.13 wasm build venv.
mkdir -p "${WASM_VENV_ROOT}"
uv venv "${WASM_VENV}" --python 3.13 --allow-existing
# shellcheck disable=SC1091
source "${WASM_VENV}/bin/activate"
uv pip install "pyodide-build==0.34.3" wheel pip

# 2. Activate emsdk.
if [[ ! -f "${EMSDK_ENV}" ]]; then
    echo "emsdk not found at ${EMSDK_ENV}."
    echo "Run: ./scripts/library_tool.sh emsdk download && compile && install"
    exit 1
fi
# shellcheck disable=SC1090
source "${EMSDK_ENV}"

# 3. Install the matching xbuildenv for pyodide-build 0.34.3. With Python 3.13
#    this resolves to the same 0.29.3 environment CI uses.
pyodide xbuildenv install

# 4. Point cmake at the wasm-target capnp/libuv install.
if [[ ! -d "${WASM_INSTALL}" ]]; then
    echo "Wasm libraries not found at ${WASM_INSTALL}."
    echo "Run: ./scripts/library_tool.sh capnp/libuv download/compile/install --target=wasm"
    exit 1
fi
export CMAKE_PREFIX_PATH="${WASM_INSTALL}"
export CapnProto_DIR="${WASM_INSTALL}/lib/cmake/CapnProto"

# 5. Build. Default to a single CMake job on low-memory machines.
rm -rf dist_wasm
CMAKE_BUILD_PARALLEL_LEVEL="${CMAKE_BUILD_PARALLEL_LEVEL:-1}" pyodide build . --outdir dist_wasm

# 6. pyodide-build 0.34.x tags wheels as pyemscripten_2025_0; Pyodide 0.29.3's
#    micropip expects emscripten_4_0_9_wasm32. Retag the freshly built wheel.
python -m wheel tags \
    --python-tag cp313 --abi-tag cp313 \
    --platform-tag emscripten_4_0_9_wasm32 \
    dist_wasm/opengris_scaler-*pyemscripten*wasm32.whl

# 7. Deploy to the docs source tree. The lite build (jupyterlite-sphinx) runs
#    during ``make html`` and reads piplite_urls from
#    docs/source/jupyter_lite_config.json — those URLs are resolved relative to
#    the config file, so the wheel(s) MUST live under docs/source/ before docs
#    build. Sphinx then copies _static/ into docs/build/html/_static/ as usual.
#    Wipe any prior wheels first to avoid the JupyterLite kernel
#    picking up a stale older-version wheel from the directory listing.
WASM_STATIC="docs/source/_static/wasm"
mkdir -p "${WASM_STATIC}"
rm -f "${WASM_STATIC}"/opengris_scaler-*wasm32.whl
cp dist_wasm/opengris_scaler-*emscripten_4_0_9*wasm32.whl "${WASM_STATIC}/"

# 8. Vendor the pure-Python runtime deps Pyodide does not bundle so the
#    JupyterLite site is fully offline-capable.
#      - cloudpickle: not in Pyodide's bundled package set
#      - tblib >= 3.2.0: Pyodide 0.29.x bundles 3.0.0, but the native worker
#        pickles exceptions via 'unpickle_exception_with_attrs' (added in 3.2.0)
#      - opengris-parfun, pargraph: pure-Python parallel-task libraries the
#        gallery notebooks import directly.
#      - bidict, loky: pure-Python pargraph runtime deps not bundled by
#        Pyodide (everything else pargraph/parfun pulls -- psutil,
#        scikit-learn, jsonschema, msgpack, pydot -- is bundled by Pyodide
#        and auto-loads on first import).
#      - attrs: parfun imports it at module load time; vendor it so the
#        first ``import parfun`` succeeds without waiting on the Pyodide
#        package auto-loader.
rm -f "${WASM_STATIC}"/cloudpickle-*.whl "${WASM_STATIC}"/tblib-*.whl \
      "${WASM_STATIC}"/opengris_parfun-*.whl "${WASM_STATIC}"/pargraph-*.whl \
      "${WASM_STATIC}"/bidict-*.whl "${WASM_STATIC}"/loky-*.whl \
      "${WASM_STATIC}"/attrs-*.whl
python -m pip download --quiet --no-deps --dest "${WASM_STATIC}" \
    "cloudpickle" "tblib>=3.2.0" "opengris-parfun" "pargraph" "bidict" "loky" "attrs"

# 9. ``jupyter_lite_config.json`` is regenerated automatically from the
#    wheels above by ``docs/source/conf.py`` during ``make html``, so it
#    does not need to live in git or be regenerated explicitly here.

echo ""
echo "Wheels deployed to ${WASM_STATIC}/"
ls -1 "${WASM_STATIC}"
echo ""
echo "Run scripts/test_jupyterlite.sh to start the cluster."