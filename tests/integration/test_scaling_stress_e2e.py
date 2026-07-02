"""Larger end-to-end scaling test: a real client bursts work and the system scales up real workers.

This simulates a small distributed system on one machine. A real scheduler starts with ZERO workers;
a client submits a burst of light tasks; the scheduler's scaling policy reacts to the backlog and
tells the dynamic native worker manager to provision worker processes; those workers connect back and
execute the tasks. It asserts both correctness (every task ran) and that the work actually spread
across multiple provisioned workers (horizontal scale-up), by counting the distinct worker PIDs that
returned results.

Heavier than the rest of the suite (spawns many processes), so it is behind its own gate
(``RUN_SCALING_STRESS_TEST=1``) and is not part of the default integration run. CI exposes it as a
manually-triggered workflow (.github/workflows/scaling-stress.yml).

Tunables (env vars, so CI / a bigger box can scale it up without code changes):
  SCALING_STRESS_MAX_WORKERS  cap the dynamic manager scales toward (default 8)
  SCALING_STRESS_TASKS        number of tasks in the burst (default 240)
  SCALING_STRESS_TASK_SECONDS per-task sleep, keeps tasks light + non-CPU-bound (default 0.2)
  SCALING_STRESS_MIN_WORKERS  minimum distinct workers the burst must spread across (default 3)
"""

import os
import sys
import unittest
from multiprocessing import get_context

from scaler import Client
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.defaults import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig
from scaler.config.types.worker import WorkerCapabilities
from scaler.utility.logging.utility import setup_logger
from tests.integration import RUN_SCALING_STRESS_TEST, SCALING_STRESS_SKIP_REASON
from tests.integration._harness import SchedulerHarness
from tests.utility.utility import logging_test_name

_MAX_WORKERS = int(os.environ.get("SCALING_STRESS_MAX_WORKERS", "8"))
_N_TASKS = int(os.environ.get("SCALING_STRESS_TASKS", "240"))
_TASK_SECONDS = float(os.environ.get("SCALING_STRESS_TASK_SECONDS", "0.2"))
_MIN_WORKERS = int(os.environ.get("SCALING_STRESS_MIN_WORKERS", "3"))


def _sleep_and_identify(value: int):
    """A deliberately light task: sleep briefly (non-CPU-bound so many can oversubscribe a few cores)
    then return the input alongside the worker's PID, which lets the test see how work was spread."""
    import time

    time.sleep(_TASK_SECONDS)
    return value, os.getpid()


def _run_native_worker_manager(scheduler_address: str, max_task_concurrency: int) -> None:
    from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

    manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_manager_id="wm-scaling-stress",
                object_storage_address=None,
                max_task_concurrency=max_task_concurrency,
            ),
            worker_config=WorkerConfig(
                per_worker_capabilities=WorkerCapabilities({}),
                per_worker_task_queue_size=10,
                heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
                task_timeout_seconds=DEFAULT_TASK_TIMEOUT_SECONDS,
                death_timeout_seconds=DEFAULT_WORKER_DEATH_TIMEOUT,
                garbage_collect_interval_seconds=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
                trim_memory_threshold_bytes=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
                hard_processor_suspend=DEFAULT_HARD_PROCESSOR_SUSPEND,
                io_threads=DEFAULT_IO_THREADS,
                event_loop="builtin",
            ),
            logging_config=LoggingConfig(paths=("/dev/stdout",), level="INFO", config_file=None),
        )
    )
    manager.run()


@unittest.skipUnless(RUN_SCALING_STRESS_TEST, SCALING_STRESS_SKIP_REASON)
@unittest.skipIf(
    sys.platform == "win32",
    "Dynamic scale-down has no graceful SIGINT path on Windows (see tests/scheduler/test_scaling.py).",
)
class TestScalingStressE2E(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.harness = SchedulerHarness(scaling_policy="allocate=even_load; scaling=vanilla")
        self.addCleanup(self.harness.shutdown)

    def _start_worker_manager(self, max_task_concurrency: int):
        process = get_context("spawn").Process(
            target=_run_native_worker_manager, args=(self.harness.scheduler_address, max_task_concurrency)
        )
        process.start()
        self.addCleanup(self._stop_process, process)
        return process

    @staticmethod
    def _stop_process(process) -> None:
        if process.is_alive():
            process.terminate()
            process.join(timeout=15)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)

    def test_burst_scales_up_across_many_workers(self) -> None:
        self._start_worker_manager(max_task_concurrency=_MAX_WORKERS)

        with Client(self.harness.scheduler_address) as client:
            results = client.map(_sleep_and_identify, range(_N_TASKS))

        # Correctness: every task ran exactly once, in order.
        self.assertEqual([value for value, _pid in results], list(range(_N_TASKS)))

        # Scale-up: starting from zero workers, the burst must have been provisioned across several.
        worker_pids = {pid for _value, pid in results}
        # print (not logging) so the scaling result is visible in the test/CI output.
        print(
            f"scaling stress: {_N_TASKS} tasks ({_TASK_SECONDS:.2f}s each) ran across "
            f"{len(worker_pids)} distinct workers (cap={_MAX_WORKERS})"
        )
        self.assertGreaterEqual(
            len(worker_pids),
            _MIN_WORKERS,
            f"expected the burst to scale up to >= {_MIN_WORKERS} workers, saw {len(worker_pids)}",
        )


if __name__ == "__main__":
    unittest.main()
