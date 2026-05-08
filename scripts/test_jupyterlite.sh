#!/usr/bin/env bash
# test_jupyterlite.sh — manual JupyterLite + wasm client debug harness.
#
# Starts a full Scaler cluster (object storage, scheduler on ws://, monitor,
# one worker) plus the docs HTTP server that serves the JupyterLite site and
# the wasm wheel.
#
# The scheduler binds to a ws:// address.  Both native CPython workers and the
# browser wasm client speak the same YMQ-over-WebSocket protocol, so a single
# bind address covers both.
#
# After this script exits you can open the URL printed below in a browser,
# navigate to send_heavy_object.ipynb (the lightweight smoke test) or any of
# the gallery notebooks, and run cells one-by-one to exercise the wasm client.
#
# Prerequisites:
#   - .venv is set up (uv pip install -e ".[all]" and dev deps: uv sync --group dev)
#   - wasm wheel has been built and deployed:
#       ./scripts/build_wasm.sh
#   - docs have been built:  cd docs && make html
#   - tmux is installed

set -euo pipefail

# Resolve repo root from the script location so this works regardless of
# the user's CWD or where the repo is checked out (devcontainer or host).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SESSION="scaler-jl"
VENV="$REPO_ROOT/.venv/bin/activate"

# Ports — match the SCHEDULER_ADDRESS = "ws://127.0.0.1:2345" placeholder
# in the gallery notebooks
SCHEDULER_WS_PORT=2345    # workers + browser wasm client both use this
OBJECT_STORAGE_PORT=2346
MONITOR_PORT=2347
DOCS_PORT=8765

# Object storage is bound on ws:// so the browser wasm client (which can
# only speak WebSocket) can reach it.  Native workers also speak the same
# YMQ-over-WebSocket protocol against the same address.
OBJECT_STORAGE_ADDR="ws://0.0.0.0:${OBJECT_STORAGE_PORT}"
OBJECT_STORAGE_CLIENT_ADDR="ws://127.0.0.1:${OBJECT_STORAGE_PORT}"
SCHEDULER_WS_ADDR="ws://0.0.0.0:${SCHEDULER_WS_PORT}"
SCHEDULER_WS_CLIENT_ADDR="ws://127.0.0.1:${SCHEDULER_WS_PORT}"
MONITOR_ADDR="tcp://127.0.0.1:${MONITOR_PORT}"

# YMQ is now the default network backend (ws:// requires it; ZMQ only understands
# tcp:// / ipc:// / inproc://).  Override SCALER_NETWORK_BACKEND only if you have
# explicitly opted out elsewhere.

tmux kill-session -t "$SESSION" 2>/dev/null || true

echo "Starting Scaler cluster for JupyterLite testing..."

# 1. Object storage server
tmux new-session -d -s "$SESSION" -n object_storage
tmux send-keys -t "$SESSION:object_storage" \
    "source $VENV && scaler_object_storage_server $OBJECT_STORAGE_ADDR" Enter

sleep 1

# 2. Scheduler — ws:// so both native workers and browser wasm client can connect.
tmux new-window -t "$SESSION" -n scheduler
tmux send-keys -t "$SESSION:scheduler" \
    "source $VENV && scaler_scheduler $SCHEDULER_WS_ADDR -osa $OBJECT_STORAGE_CLIENT_ADDR -ma $MONITOR_ADDR" Enter

sleep 1

# 3. Monitor UI
tmux new-window -t "$SESSION" -n ui
tmux send-keys -t "$SESSION:ui" \
    "source $VENV && scaler_gui $MONITOR_ADDR" Enter

sleep 2

# 4. One worker — also connects via ws://
tmux new-window -t "$SESSION" -n worker
tmux send-keys -t "$SESSION:worker" \
    "source $VENV && scaler_worker_manager baremetal_native $SCHEDULER_WS_CLIENT_ADDR --worker-manager-id jl_worker --max-task-concurrency 4" Enter

sleep 2

# 5. Docs HTTP server — serves JupyterLite + wasm wheel
tmux new-window -t "$SESSION" -n docs_server
tmux send-keys -t "$SESSION:docs_server" \
    "source $VENV && cd $REPO_ROOT/docs/build/html && python -m http.server $DOCS_PORT" Enter

sleep 1

echo ""
echo "======================================================================"
echo "  JupyterLite debug environment ready"
echo "======================================================================"
echo ""
echo "  Scaler monitor UI  : http://localhost:50001"
echo "  JupyterLite site   : http://localhost:${DOCS_PORT}/lite/lab/index.html"
echo "  Debug notebook     : http://localhost:${DOCS_PORT}/lite/lab/index.html?path=send_heavy_object.ipynb"
echo ""
echo "  Scheduler (workers + browser wasm): ${SCHEDULER_WS_CLIENT_ADDR}"
echo "  Object storage (workers + browser wasm): ${OBJECT_STORAGE_CLIENT_ADDR}"
echo ""
echo "  NOTE: set SCHEDULER_ADDRESS = '${SCHEDULER_WS_CLIENT_ADDR}' in the notebook."
echo "        The scheduler advertises object storage at '${OBJECT_STORAGE_CLIENT_ADDR}'."
echo "        The wasm wheel must be at docs/build/html/_static/wasm/ before running."
echo ""
echo "  To attach to tmux : tmux attach -t $SESSION"
echo "  To stop everything: tmux kill-session -t $SESSION"
echo "======================================================================"
