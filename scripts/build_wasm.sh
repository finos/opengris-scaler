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

# 8. Vendor / build the runtime deps the JupyterLite kernel pulls at boot.
#    Wheels are split into three groups:
#      a) Real pure-Python wheels from PyPI (cloudpickle, tblib>=3.2.0,
#         opengris-parfun, pargraph, bidict, pydot). cloudpickle/tblib are
#         vendored even though Pyodide bundles older copies because the
#         scaler worker pickles exceptions via tblib 3.2.0+ APIs.
#      b) Stub wheels built from scripts/wasm_stubs/ (psutil, loky). Pyodide
#         0.29 does not bundle psutil and the upstream package has no
#         pure-Python wheel; loky imports multiprocessing.synchronize at
#         module load which needs the _multiprocessing C extension Pyodide
#         lacks. The stubs satisfy the imports parfun/pargraph perform at
#         module load (psutil.cpu_count, loky.get_reusable_executor) so the
#         gallery notebooks can ``import parfun`` and ``import pargraph``.
#      c) Pyodide-bundled deps (attrs, jsonschema, msgpack, scikit-learn).
#         These are NOT vendored: piplite.install resolves them from
#         Pyodide's own pyodide-lock.json at boot. We list them in PACKAGES
#         (in patch_jupyterlite_kernel.py) so the resolution happens
#         explicitly before parfun/pargraph import them.
rm -f "${WASM_STATIC}"/cloudpickle-*.whl "${WASM_STATIC}"/tblib-*.whl \
      "${WASM_STATIC}"/opengris_parfun-*.whl "${WASM_STATIC}"/pargraph-*.whl \
      "${WASM_STATIC}"/bidict-*.whl "${WASM_STATIC}"/pydot-*.whl \
      "${WASM_STATIC}"/psutil-*.whl "${WASM_STATIC}"/loky-*.whl \
      "${WASM_STATIC}"/attrs-*.whl
python -m pip download --quiet --no-deps --dest "${WASM_STATIC}" \
    "cloudpickle" "tblib>=3.2.0" "opengris-parfun" "pargraph" "bidict" "pydot"
for stub in psutil loky; do
    python -m pip wheel --quiet --no-deps \
        --wheel-dir "${WASM_STATIC}" "scripts/wasm_stubs/${stub}"
done

# 8b. Smoke-test that the vendored + stub wheels are importable inside a
#     Pyodide virtualenv. This catches the most common breakage class --
#     a transitive dep that imports a missing C extension at module load --
#     before the wheels reach end users in the browser. Skipped if the
#     pyodide CLI is unavailable (e.g. the dedicated wasm venv was wiped).
if command -v pyodide >/dev/null 2>&1; then
    SMOKE_PARENT="$(mktemp -d)"
    SMOKE_VENV="${SMOKE_PARENT}/pyo-smoke"
    SMOKE_WHEELS="${PWD}/${WASM_STATIC}"
    pyodide venv "${SMOKE_VENV}" >/dev/null
    # Pyodide pip ignores relative --find-links, hence the absolute path.
    # Stand-in for the Pyodide-bundled deps (attrs, jsonschema, msgpack,
    # numpy, scikit-learn, pyparsing, ...): pull from PyPI in the local
    # smoke env so the imports below resolve. In the browser these come
    # from pyodide-lock.json instead, but the import surface is the same.
    "${SMOKE_VENV}/bin/pip" install --quiet \
        attrs jsonschema msgpack numpy scikit-learn pyparsing
    "${SMOKE_VENV}/bin/pip" install --quiet --no-index --find-links "${SMOKE_WHEELS}" \
        cloudpickle "tblib>=3.2.0" bidict pydot psutil loky
    "${SMOKE_VENV}/bin/pip" install --quiet --no-deps --no-index \
        --find-links "${SMOKE_WHEELS}" opengris-parfun pargraph
    # Pyodide's CLI sometimes raises a benign TypeError from its shutdown
    # excepthook after a successful run; check for the OK marker on stdout
    # rather than trusting the process exit code.
    SMOKE_OUT="$("${SMOKE_VENV}/bin/python" -c "
import psutil, loky, bidict, pydot, cloudpickle, tblib
import attrs, jsonschema, msgpack, numpy, sklearn
import parfun, pargraph
# Exercise the psutil surface scaler's client heartbeat manager touches
# every loop iteration. The stub returns zeros; what we are checking is
# that the calls do not raise AttributeError.
proc = psutil.Process()
assert isinstance(proc.cpu_percent(), float)
assert isinstance(proc.memory_info().rss, int)
assert isinstance(psutil.virtual_memory().available, int)
print('wasm import smoke test: OK (psutil.cpu_count={})'.format(psutil.cpu_count()))
" 2>&1 || true)"
    echo "${SMOKE_OUT}"
    if ! echo "${SMOKE_OUT}" | grep -q "wasm import smoke test: OK"; then
        echo "wasm import smoke test FAILED" >&2
        rm -rf "${SMOKE_PARENT}"
        exit 1
    fi
    rm -rf "${SMOKE_PARENT}"
fi

# 9. ``jupyter_lite_config.json`` is regenerated automatically from the
#    wheels above by ``docs/source/conf.py`` during ``make html``, so it
#    does not need to live in git or be regenerated explicitly here.

echo ""
echo "Wheels deployed to ${WASM_STATIC}/"
ls -1 "${WASM_STATIC}"
echo ""
echo "Run scripts/test_jupyterlite.sh to start the cluster."