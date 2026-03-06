# Amendment 1: Simplified FIXED Mode, Config Rename, and `--worker-type` Parameter

| Field | Value |
|---|---|
| Author | @magniloquency |
| Pull Request | #583 |
| Amends | [plan.md](plan.md) |

**Status**: Implemented

This amendment supersedes and extends several parts of the plan with three groups of changes:

1. **Simplified FIXED mode run path.** The plan's Step 2 routes both modes through the same async
   event loop, using `_monitor_workers()` and `max_worker_groups=0` heartbeats to make FIXED mode
   passive. This amendment replaces that approach: FIXED mode takes a completely separate,
   synchronous code path in `run()` with no event loop, no ZMQ connector, and no heartbeats.
   Workers connect directly to the scheduler themselves; the adapter's only responsibility in
   FIXED mode is to spawn them and wait for them to finish. All FIXED-specific branches inside the
   async machinery become dead code and are removed.

2. **Config and TOML section rename.** `NativeWorkerAdapterConfig` is renamed to
   `NativeWorkerManagerConfig` and the TOML section key changes from `[native_worker_adapter]` to
   `[native_worker_manager]`. The TOML rename is a breaking change for existing configuration files.

3. **New `--worker-type` parameter.** An optional `worker_type` field is added to
   `NativeWorkerManagerConfig` so callers can override the worker ID prefix (default: `NAT` for
   DYNAMIC mode, `FIX` for FIXED mode). `ECSWorkerAdapter` uses this to pass `--worker-type ECS`,
   making ECS workers identifiable in logs and monitoring.

## Revised `run()` and `_run_fixed()`

```python
def run(self) -> None:
    if self._mode == NativeWorkerManagerMode.FIXED:
        self._run_fixed()
        return

    # DYNAMIC mode — unchanged
    self._setup_zmq()
    self._loop = asyncio.new_event_loop()
    run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

def _run_fixed(self) -> None:
    setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
    self._spawn_initial_workers()
    for group in self._worker_groups.values():
        for worker in group.values():
            worker.join()
```

## Code to remove

Because FIXED mode no longer participates in the event loop, all FIXED-specific branches
inside the async machinery are dead code and must be removed:

| Location | What to remove |
|---|---|
| `start_worker_group()` | `if self._mode == NativeWorkerManagerMode.FIXED: return b"", Status.WorkerGroupTooMuch` |
| `shutdown_worker_group()` | `if self._mode == NativeWorkerManagerMode.FIXED: return Status.WorkerGroupIDNotFound` |
| `__send_heartbeat()` | `max_worker_groups = 0 if ... FIXED else ...` — simplify to always use `self._max_workers` |
| `__get_loops()` | `if self._mode == NativeWorkerManagerMode.FIXED: loops.append(self._monitor_workers())` |
| `_cleanup()` | FIXED worker termination block (workers are joined in `_run_fixed()`; `_cleanup` is DYNAMIC-only) |
| `_monitor_workers()` | Delete the entire method |

## New `--worker-type` parameter

Add an optional `worker_type` field to `NativeWorkerManagerConfig` that allows callers to
override the worker ID prefix:

```python
# src/scaler/config/section/native_worker_adapter.py
worker_type: Optional[str] = dataclasses.field(
    default=None,
    metadata=dict(help="worker type prefix used in worker IDs; defaults to 'FIX' or 'NAT' based on mode"),
)
```

In `NativeWorkerAdapter.__init__`, replace the mode-based prefix assignment with:

```python
if config.worker_type is not None:
    self._worker_prefix = config.worker_type
elif self._mode == NativeWorkerManagerMode.FIXED:
    self._worker_prefix = "FIX"
elif self._mode == NativeWorkerManagerMode.DYNAMIC:
    self._worker_prefix = "NAT"
else:
    raise ValueError(f"worker_type is not set and mode is unrecognised: {self._mode!r}")
```

The `worker_type` value is passed through unchanged — no normalisation or case conversion.
Validation (e.g. rejecting empty strings) is intentionally omitted unless requirements emerge.

## Rename `NativeWorkerAdapterConfig` → `NativeWorkerManagerConfig`

Rename the config class and update every import and usage site:

```bash
# Files requiring the rename
src/scaler/config/section/native_worker_adapter.py      # class definition
src/scaler/worker_manager_adapter/baremetal/native.py   # import + type annotation
src/scaler/cluster/combo.py                             # import + instantiation
src/scaler/entry_points/cluster.py                      # import + instantiation
src/scaler/entry_points/worker_manager_baremetal_native.py  # import + instantiation
tests/client/test_client.py
tests/scheduler/test_balance.py
tests/scheduler/test_capabilities.py
tests/scheduler/test_scaling.py
tests/cluster/test_cluster_disconnect.py
tests/core/test_death_timeout.py
examples/task_capabilities.py
```

The CLI tool name (`scaler_worker_manager_baremetal_native`) is defined by a `pyproject.toml` entry point
and is already consistent with the `manager` naming. The TOML section name is defined by the string
argument passed to `.parse()` and **must be renamed separately** — see [Rename TOML section name](#rename-toml-section-name) below.

Verify no stale references remain:

```bash
grep -r "NativeWorkerAdapterConfig" src/ tests/ examples/ docs/ README.md
```

## Rename TOML section name

Rename the TOML section key from `native_worker_adapter` to `native_worker_manager` to match the
renamed class. This is a **breaking change** for existing TOML configuration files — users must
update their `[native_worker_adapter]` sections to `[native_worker_manager]`.

### Entry point

In `src/scaler/entry_points/worker_manager_baremetal_native.py`, update the `.parse()` call:

```python
# before
native_adapter_config = NativeWorkerManagerConfig.parse("Scaler Native Worker Adapter", "native_worker_adapter")

# after
native_adapter_config = NativeWorkerManagerConfig.parse("Scaler Native Worker Adapter", "native_worker_manager")
```

### Documentation

Update all TOML snippets in `docs/source/tutorials/worker_adapters/native.rst` and
`docs/source/tutorials/configuration.rst` that reference `[native_worker_adapter]` to use
`[native_worker_manager]`.

Verify no stale section name references remain:

```bash
grep -r "native_worker_adapter" src/ tests/ examples/ docs/ README.md
```

## `ECSWorkerAdapter` — pass `--worker-type ECS`

**File:** `src/scaler/worker_manager_adapter/aws_raw/ecs.py`

`ECSWorkerAdapter.start_worker_group()` launches workers via `scaler_cluster`, which after Step 3
creates a `NativeWorkerAdapter` in FIXED mode. Without an explicit `--worker-type`, FIXED mode
defaults to the `FIX` prefix. Add `--worker-type ECS` so ECS workers are identifiable by type in
logs and monitoring:

```python
command = (
    f"scaler_cluster {self._address.to_address()} "
    f"--worker-type ECS "
    f"--max-workers {self._ecs_task_cpu} "
    ...
)
```

## Documentation updates

Add a `--worker-type` entry to `docs/source/tutorials/worker_adapters/native.rst` in the
parameter reference table/list alongside `--mode`:

```
* ``--worker-type``: Optional string prefix used in worker IDs. Overrides the default
  prefix (``NAT`` for dynamic mode, ``FIX`` for fixed mode). Useful when multiple
  adapters of the same mode are running concurrently and their workers need to be
  distinguishable by type in logs and monitoring.
```

## Validation

Run the full verification suite from the original plan:

```bash
# No stale references to old class name
grep -r "NativeWorkerAdapterConfig" src/ tests/ examples/ docs/ README.md

# No stale references to old TOML section name
grep -r "native_worker_adapter" src/ tests/ examples/ docs/ README.md

# No stale references to removed fixed-native adapter
grep -r "FixedNative\|fixed_native_worker_adapter" src/ tests/
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

# Smoke test CLI
scaler_cluster --help
scaler_worker_manager_baremetal_native --help
```
