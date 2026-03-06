# Plan: Merge FixedNativeWorkerAdapter into NativeWorkerAdapter

| Field | Value |
|---|---|
| Author | @magniloquency |
| Pull Request | #583 |

**Status**: Implemented, superceded by [amendment 1](amendment-1.md)

## Context

`FixedNativeWorkerAdapter` was based on an old version of `NativeWorkerAdapter` and has diverged. Rather than
maintaining two separate adapters, we merge FIXED mode into `NativeWorkerAdapter` via a `NativeWorkerManagerMode`
enum. This simplifies the codebase and keeps both modes in one place.

## Enum

Add `NativeWorkerManagerMode` enum to `native_worker_adapter.py`:
```python
class NativeWorkerManagerMode(enum.Enum):
    DYNAMIC = "dynamic"
    FIXED = "fixed"
```

## Step 1 — Update `NativeWorkerAdapterConfig`

**File:** `src/scaler/config/section/native_worker_adapter.py`

- Import `argparse`, `enum`
- Add `NativeWorkerManagerMode` enum (see above)
- Add field: `mode: NativeWorkerManagerMode = dataclasses.field(default=NativeWorkerManagerMode.DYNAMIC, metadata=dict(...))`
- Add `configure_parser` with `-n`/`--num-of-workers` alias (from `FixedNativeWorkerAdapterConfig`):
  ```python
  @classmethod
  def configure_parser(cls, parser) -> None:
      super().configure_parser(parser)
      parser.add_argument("-n", "--num-of-workers", dest="max_workers", type=int, help=argparse.SUPPRESS)
  ```
- In `__post_init__`: add `if self.mode == NativeWorkerManagerMode.FIXED and self.worker_adapter_config.max_workers < 0: raise ValueError(...)` — the threshold is `< 0` rather than `< 1` to allow `max_workers=0`, which `SchedulerClusterCombo(n_workers=0)` relies on (several tests create a combo with zero workers and then manually attach adapters)

## Step 2 — Update `NativeWorkerAdapter`

**File:** `src/scaler/worker_manager_adapter/baremetal/native.py`

Key design decisions:
- **Single `run()` path**: both modes use the same async event loop. No `start()`/`shutdown()`/`join()` interface.
- **ZMQ is lazy**: move `create_async_simple_context()` / `create_async_connector()` out of `__init__` into `_setup_zmq()` called at the top of `run()`. This keeps the object picklable so callers can do `Process(target=adapter.run).start()`.
- **Deferred fields typed for mypy**: `_context` and `_connector_external` are initialised to `None` in `__init__` and assigned in `_setup_zmq()`. They must be typed as `Optional[Any]` and `Optional[AsyncConnector]` respectively (not `Optional[object]`); otherwise mypy cannot resolve `.send()`, `.routine`, and `.destroy()` calls on lines that follow.
- **FIXED pre-spawns workers**: before entering the event loop, FIXED mode spawns all N workers via `_spawn_initial_workers()`.
- **FIXED rejects dynamic commands**: `start_worker_group()` returns `WorkerGroupTooMuch`; `shutdown_worker_group()` returns `WorkerGroupIDNotFound`.
- **FIXED heartbeat**: advertises `max_worker_groups=0` so the scheduler never sends dynamic commands.
- **FIXED self-exits when workers die**: a `_monitor_workers()` routine in the event loop cancels the loop when all workers have exited, preserving the semantics needed by `test_death_timeout`.
- **FIXED cleanup**: `_cleanup()` terminates and joins any remaining workers.
- **Worker prefix**: `"FIX|"` in FIXED mode, `"NAT|"` in DYNAMIC mode.
- **Extract `_create_worker()`** helper to eliminate duplication.

Changes:

```python
def __init__(self, config):
    ...
    self._mode = config.mode
    if self._mode == NativeWorkerManagerMode.FIXED:
        self._worker_prefix = "FIX"
    elif self._mode == NativeWorkerManagerMode.DYNAMIC:
        self._worker_prefix = "NAT"
    else:
        raise ValueError(f"Unknown NativeWorkerManagerMode: {self._mode!r}")
    self._worker_groups: Dict[WorkerGroupID, Dict[WorkerID, Worker]] = {}
    # ZMQ setup removed from here — moved to _setup_zmq()

def _create_worker(self) -> Worker:
    return Worker(name=f"{self._worker_prefix}|{uuid.uuid4().hex}", ...)

def _spawn_initial_workers(self) -> None:
    for _ in range(self._max_workers):
        worker = self._create_worker()
        worker.start()
        group_id = f"fixed-{uuid.uuid4().hex}".encode()
        self._worker_groups[group_id] = {worker.identity: worker}

def run(self) -> None:
    if self._mode == NativeWorkerManagerMode.FIXED:
        self._spawn_initial_workers()
    self._setup_zmq()
    self._loop = asyncio.new_event_loop()
    run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

def _setup_zmq(self) -> None:
    # existing __init__ ZMQ setup code moved here

def _cleanup(self) -> None:
    if self._connector_external is not None:
        self._connector_external.destroy()
    if self._mode == NativeWorkerManagerMode.FIXED:
        for group in self._worker_groups.values():
            for worker in group.values():
                worker.terminate()
                worker.join()

async def start_worker_group(self) -> Tuple[WorkerGroupID, Status]:
    if self._mode == NativeWorkerManagerMode.FIXED:
        return b"", Status.WorkerGroupTooMuch
    # ... existing DYNAMIC logic unchanged

async def shutdown_worker_group(self, worker_group_id: WorkerGroupID) -> Status:
    if self._mode == NativeWorkerManagerMode.FIXED:
        return Status.WorkerGroupIDNotFound
    # ... existing DYNAMIC logic unchanged

async def __send_heartbeat(self) -> None:
    max_worker_groups = 0 if self._mode == NativeWorkerManagerMode.FIXED else self._max_workers
    await self._connector_external.send(
        WorkerAdapterHeartbeat.new_msg(
            max_worker_groups=max_worker_groups,
            workers_per_group=self._workers_per_group,
            capabilities=self._capabilities,
        )
    )

async def _monitor_workers(self) -> None:
    """FIXED mode only: cancel the event loop when all pre-spawned workers have exited."""
    while True:
        await asyncio.sleep(1)
        if self._worker_groups and all(not w.is_alive() for group in self._worker_groups.values() for w in group.values()):
            self.__destroy()
            return

async def __get_loops(self) -> None:
    loops = [
        create_async_loop_routine(self._connector_external.routine, 0),
        create_async_loop_routine(self.__send_heartbeat, self._heartbeat_interval_seconds),
    ]
    if self._mode == NativeWorkerManagerMode.FIXED:
        loops.append(self._monitor_workers())
    # ... rest unchanged
```

Use `_create_worker()` inside existing `start_worker_group()` for DYNAMIC mode.

## Step 3 — Update cluster entry point; delete fixed_native entry point and run script

**File:** `src/scaler/entry_points/cluster.py`

Currently imports from the deleted entry point. Replace with a direct call to the merged adapter:

```python
import dataclasses

from scaler.config.section.native_worker_adapter import NativeWorkerAdapterConfig, NativeWorkerManagerMode
from scaler.worker_manager_adapter.baremetal.native import NativeWorkerAdapter


def main() -> None:
    config = NativeWorkerAdapterConfig.parse("Scaler Cluster", "cluster")
    config = dataclasses.replace(config, mode=NativeWorkerManagerMode.FIXED)
    NativeWorkerAdapter(config).run()


__all__ = ["main"]
```

`src/run_cluster.py` requires no changes.

Then delete the now-superseded files:
- `src/scaler/entry_points/worker_manager_baremetal_fixed_native.py`
- `src/run_worker_manager_baremetal_fixed_native.py`

Also remove the corresponding `console_scripts` entry from `setup.cfg`/`pyproject.toml` if present.

## Step 4 — Update `combo.py`

**File:** `src/scaler/cluster/combo.py`

Replace `FixedNativeWorkerAdapter`/`FixedNativeWorkerAdapterConfig` with `NativeWorkerAdapter`/`NativeWorkerAdapterConfig`/`NativeWorkerManagerMode`. Spawn the adapter as a subprocess since `run()` now blocks in the event loop.

Do **not** pass `daemon=True` to `multiprocessing.Process` — Python forbids daemon processes from spawning child processes, and since the adapter subprocess calls `_spawn_initial_workers()` which calls `worker.start()`, using `daemon=True` raises `AssertionError: daemonic processes are not allowed to have children`. This is safe because `shutdown()` explicitly calls `terminate()` + `join()`.

Store the adapter instance as `self._worker_adapter` rather than discarding it after constructing the `Process`. Several tests access `combo._worker_adapter._address`, `._event_loop`, `._io_threads`, etc. to mirror config values when constructing additional adapters. Since ZMQ setup is deferred to `run()`, the object is picklable and the subprocess receives a copy; the main-process reference is used read-only for config attributes.

```python
import multiprocessing

self._worker_adapter = NativeWorkerAdapter(config)
self._worker_adapter_process = multiprocessing.Process(target=self._worker_adapter.run)
self._worker_adapter_process.start()
```

Teardown in `shutdown()`:
```python
if self._worker_adapter_process.is_alive():
    self._worker_adapter_process.terminate()
self._worker_adapter_process.join()
```

The `is_alive()` guard is necessary because in FIXED mode the adapter can self-exit (via `_monitor_workers()`) before `shutdown()` is called, and calling `terminate()` on an already-dead process raises `OSError`.

## Step 5 — Update tests (5 files)

All tests replace `adapter.start()`/`adapter.shutdown()`/`adapter.join()` with a subprocess pattern:

```python
process = multiprocessing.Process(target=NativeWorkerAdapter(config).run)
process.start()
# ... test body ...
process.terminate()
process.join()
```

**Do not use `daemon=True`** — for the same reason as in `combo.py`: the adapter process spawns worker subprocesses, which Python forbids from daemon processes.

`test_death_timeout.test_no_scheduler` previously called `adapter.join()` to block until workers self-exited. With the event loop, the adapter self-exits when `_monitor_workers()` detects all workers have died — so `process.join()` still works correctly here.

Files:
- `tests/client/test_client.py`
- `tests/scheduler/test_balance.py`
- `tests/scheduler/test_capabilities.py`
- `tests/cluster/test_cluster_disconnect.py`
- `tests/core/test_death_timeout.py`

## Step 6 — Delete old files

```bash
git rm src/scaler/worker_manager_adapter/baremetal/fixed_native.py
git rm src/scaler/config/section/fixed_native_worker_adapter.py
git rm src/scaler/entry_points/worker_manager_baremetal_fixed_native.py
git rm src/run_worker_manager_baremetal_fixed_native.py
```

Verify no remaining references: `grep -r "FixedNative\|fixed_native_worker_adapter\|fixed_native" src/ tests/`

## Step 7 — Update documentation

### `docs/source/tutorials/worker_adapters/native.rst`

Extend the existing Native adapter docs to cover both modes:

- Rename the "Native Configuration" subsection to "Configuration" (or keep as-is and add a new subsection).
- Add a `--mode` parameter entry:
  ```
  * ``--mode``: Operating mode. ``dynamic`` (default) enables auto-scaling driven by the scheduler.
    ``fixed`` pre-spawns ``--max-workers`` workers at startup and does not support dynamic scaling.
    In fixed mode ``--max-workers`` must be a positive integer.
  ```
- Update the `--max-workers` description to note the semantic difference between modes:
  - DYNAMIC: maximum workers that can be spawned (``-1`` = unlimited, default)
  - FIXED: exact number of workers spawned at startup (must be ≥ 1)
- Update the "How it Works" section to cover both code paths:
  - DYNAMIC: scheduler-driven on-demand spawning (existing text)
  - FIXED: all workers pre-spawned at startup; adapter exits when all workers have died; scheduler sees capacity 0 and never sends scale commands
- Update the TOML example to show `mode = "fixed"` variant:
  ```toml
  [native_worker_adapter]
  mode = "fixed"
  max_workers = 8
  ```
- Remove the sentence "Unlike the Fixed Native worker adapter, which spawns a static number of workers at startup..."

### `docs/source/tutorials/worker_adapters/fixed_native.rst`

**Delete this file** — its content is superseded by the updated `native.rst`.

```bash
git rm docs/source/tutorials/worker_adapters/fixed_native.rst
```

### `docs/source/tutorials/worker_adapters/index.rst`

- Remove the "Fixed Native" subsection (lines 39–42) and the `fixed_native` toctree entry.
- Update the Native subsection description to mention it supports both dynamic and fixed modes:
  > The :doc:`Native <native>` worker adapter provisions workers as local subprocesses on the same machine.
  > It supports both dynamic auto-scaling (default) and fixed-pool mode, where a set number of workers
  > are pre-spawned at startup.

### `docs/source/tutorials/configuration.rst`

Remove the `scaler_worker_adapter_fixed_native` / `[fixed_native_worker_adapter]` row from the CLI-to-TOML-section table.

### `README.md`

Remove the `scaler_worker_adapter_fixed_native` / `[fixed_native_worker_adapter]` row from the TOML section names table (line ~251).

## Files Modified

| File | Change |
|---|---|
| `src/scaler/config/section/native_worker_adapter.py` | Add enum, `mode` field, `-n` alias, validation |
| `src/scaler/worker_manager_adapter/baremetal/native.py` | Unified event loop for both modes; lazy ZMQ; `_create_worker()`; FIXED: pre-spawn, reject commands, monitor workers |
| `src/scaler/entry_points/cluster.py` | Import from merged adapter directly |
| `src/scaler/cluster/combo.py` | Use `NativeWorkerAdapter(mode=FIXED)` |
| `tests/client/test_client.py` | Update imports + subprocess pattern |
| `tests/scheduler/test_balance.py` | Update imports + subprocess pattern |
| `tests/scheduler/test_capabilities.py` | Update imports + subprocess pattern (x2) |
| `tests/cluster/test_cluster_disconnect.py` | Update imports + subprocess pattern |
| `tests/core/test_death_timeout.py` | Update imports + subprocess pattern |
| `docs/source/tutorials/worker_adapters/native.rst` | Document `--mode`; update `--max-workers`; cover both modes in "How it Works"; add fixed TOML example |
| `docs/source/tutorials/worker_adapters/index.rst` | Remove Fixed Native section and toctree entry; update Native description |
| `docs/source/tutorials/configuration.rst` | Remove `fixed_native` row from CLI-to-TOML table |
| `README.md` | Remove `fixed_native` row from TOML section names table |
| `examples/task_capabilities.py` | Replace stale `Cluster`/`ClusterConfig` with `NativeWorkerAdapter`/`NativeWorkerAdapterConfig`; access config via `cluster._worker_adapter`; subprocess pattern |
| `src/scaler/worker_manager_adapter/baremetal/fixed_native.py` | **Delete** |
| `src/scaler/config/section/fixed_native_worker_adapter.py` | **Delete** |
| `src/scaler/entry_points/worker_manager_baremetal_fixed_native.py` | **Delete** |
| `src/run_worker_manager_baremetal_fixed_native.py` | **Delete** |
| `docs/source/tutorials/worker_adapters/fixed_native.rst` | **Delete** |

`src/run_cluster.py` — no changes needed (calls `cluster.main()` which still works).

## Verification

### Environment setup (@magniloquency)

```bash
source .env
```

### Checks

```bash
# Check no remaining references to old classes
grep -r "FixedNative\|fixed_native_worker_adapter" src/ tests/

# Check no remaining references in docs
grep -r "fixed_native\|Fixed Native" docs/ README.md

# Build documentation
cd docs/ && make html

# Formatting
isort --check src/ tests/ examples/
black --check src/ tests/ examples/

# Linting
pflake8 src/ tests/ examples/

# Type checking
mypy src/ tests/ examples/

# Run affected tests
python -m unittest \
    tests/client/test_client.py \
    tests/scheduler/test_balance.py \
    tests/scheduler/test_capabilities.py \
    tests/cluster/test_cluster_disconnect.py \
    tests/core/test_death_timeout.py

# Note: test_noop_map (TestClient) fails with TimeoutError on both main and this branch.
# This is a pre-existing, environment-specific flaky test — not introduced by this change.

# Smoke test CLI via cluster entry point (fixed native now launched through cluster)
scaler_cluster --help
```

## Future Work

### Runtime worker adapter selection in `SchedulerClusterCombo`

Because `combo.py` now launches the worker adapter as a subprocess via `multiprocessing.Process(target=adapter.run)`, the adapter is decoupled from the combo's own lifecycle. This opens the door to letting callers select which worker adapter to use at runtime rather than hard-coding `NativeWorkerAdapter` in FIXED mode.

A natural next step would be to introduce an `adapter` parameter (or similar) to `SchedulerClusterCombo.__init__` that accepts any adapter instance exposing a `run()` method. The combo would simply pass `target=adapter.run` to the subprocess, making it adapter-agnostic. This would allow users to plug in, for example, an AWS HPC adapter or a future adapter without subclassing `SchedulerClusterCombo`.
