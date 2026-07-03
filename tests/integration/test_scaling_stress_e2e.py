"""Larger end-to-end scaling tests: a real client bursts work and the system scales up real workers,
across one and across many worker managers ("machines").

This simulates a small distributed system on one physical machine:

* ``TestScalingStressE2E`` -- one dynamic native worker manager: a scheduler starts with ZERO workers,
  a client bursts light tasks, and the scheduler scales that manager up to provision worker processes
  that run the work (asserted via the distinct worker PIDs seen in the results).
* ``TestMultiManagerScalingE2E`` -- several worker managers, one per simulated "machine", each
  provisioning its own workers and all attached to one scheduler. This mirrors production, where a
  cloud manager (ECS/ORB) provisions an instance whose user-data launches a `scaler_worker_manager
  baremetal_native` that runs that machine's workers. It checks that work spreads across multiple
  machines, and that provisioning a NEW machine mid-flight adds capacity that picks up work. Each
  manager tags its workers with SCALER_IT_MACHINE_ID (inherited by its worker/processor procs) so a
  task can report which machine ran it.

Heavier than the rest of the suite (spawns many processes), so these are behind their own gate
(``RUN_SCALING_STRESS_TEST=1``) and are not part of the default integration run. CI exposes them as a
manually-triggered workflow (.github/workflows/scaling-stress.yml).

Tunables (env vars, so CI / a bigger box can scale them up without code changes):
  SCALING_STRESS_MAX_WORKERS         cap the single dynamic manager scales toward (default 8)
  SCALING_STRESS_TASKS               number of tasks in a burst (default 240)
  SCALING_STRESS_TASK_SECONDS        per-task sleep, keeps tasks light + non-CPU-bound (default 0.2)
  SCALING_STRESS_MIN_WORKERS         minimum distinct workers the single-manager burst spreads across (default 3)
  SCALING_STRESS_MACHINES            number of machines / worker managers (default 3)
  SCALING_STRESS_WORKERS_PER_MACHINE per-machine worker cap (default 2)
"""

import os
import sys
import unittest
from multiprocessing import get_context
from typing import Optional

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


def _identify_machine(value: int):
    """Light task that also reports the SCALER_IT_MACHINE_ID inherited from the worker manager that
    provisioned this worker, so the test can see the work spread across multiple 'machines'."""
    import time

    time.sleep(_TASK_SECONDS)
    return value, os.environ.get("SCALER_IT_MACHINE_ID", "?"), os.getpid()


def _run_native_worker_manager(
    scheduler_address: str,
    max_task_concurrency: int,
    worker_manager_id: str = "wm-scaling-stress",
    machine_id: Optional[str] = None,
) -> None:
    from scaler.worker_manager_adapter.baremetal.native import NativeWorkerManager

    # Tag this "machine" so tasks can report which manager provisioned the worker that ran them.
    # Set before the manager spawns workers so the value is inherited by the worker/processor procs.
    if machine_id is not None:
        os.environ["SCALER_IT_MACHINE_ID"] = machine_id

    manager = NativeWorkerManager(
        NativeWorkerManagerConfig(
            worker_manager_config=WorkerManagerConfig(
                scheduler_address=AddressConfig.from_string(scheduler_address),
                worker_manager_id=worker_manager_id,
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


_NUM_MACHINES = int(os.environ.get("SCALING_STRESS_MACHINES", "3"))
_WORKERS_PER_MACHINE = int(os.environ.get("SCALING_STRESS_WORKERS_PER_MACHINE", "2"))


@unittest.skipUnless(RUN_SCALING_STRESS_TEST, SCALING_STRESS_SKIP_REASON)
@unittest.skipIf(
    sys.platform == "win32",
    "Dynamic scale-down has no graceful SIGINT path on Windows (see tests/scheduler/test_scaling.py).",
)
class TestMultiManagerScalingE2E(unittest.TestCase):
    """Multiple worker managers (one per 'machine') on one scheduler, each provisioning its own
    workers; see the module docstring."""

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.harness = SchedulerHarness(scaling_policy="allocate=even_load; scaling=vanilla")
        self.addCleanup(self.harness.shutdown)

    def _start_machine(self, machine_id: str, max_task_concurrency: int):
        process = get_context("spawn").Process(
            target=_run_native_worker_manager,
            args=(self.harness.scheduler_address, max_task_concurrency, f"wm-{machine_id}", machine_id),
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

    def test_work_spreads_across_multiple_machines(self) -> None:
        """Several machines, each a separate worker manager with its own workers, share the load."""
        for index in range(_NUM_MACHINES):
            self._start_machine(f"machine-{index}", max_task_concurrency=_WORKERS_PER_MACHINE)

        with Client(self.harness.scheduler_address) as client:
            results = client.map(_identify_machine, range(_N_TASKS))

        self.assertEqual([value for value, _machine, _pid in results], list(range(_N_TASKS)))

        machines = {machine for _value, machine, _pid in results if machine != "?"}
        pids_per_machine = {m: len({pid for _v, mm, pid in results if mm == m}) for m in machines}
        print(
            f"multi-manager: {_N_TASKS} tasks ran across machines {sorted(machines)} "
            f"(distinct workers per machine: {pids_per_machine})"
        )
        # Work provisioned by at least two separate managers, each having brought up its own workers.
        self.assertGreaterEqual(len(machines), 2, f"work only ran on machines {machines}; expected >= 2")

    def test_provisioning_new_machine_adds_capacity(self) -> None:
        """Start one machine, then provision a second machine mid-flight and prove the new machine's
        newly-provisioned workers pick up work."""
        import time

        self._start_machine("machine-0", max_task_concurrency=_WORKERS_PER_MACHINE)

        with Client(self.harness.scheduler_address) as client:
            # Warm up: only machine-0 exists, so it must handle this batch.
            warmup = client.map(_identify_machine, range(_N_TASKS // 4))
            self.assertEqual({m for _v, m, _p in warmup if m != "?"}, {"machine-0"})

            # Provision a NEW machine (a second worker manager, as a cloud manager would on scale-up).
            self._start_machine("machine-1", max_task_concurrency=_WORKERS_PER_MACHINE)

            # Keep feeding work until the new machine's provisioned workers contribute (bounded).
            saw_new_machine = False
            deadline = time.monotonic() + 60.0
            while not saw_new_machine and time.monotonic() < deadline:
                batch = client.map(_identify_machine, range(_N_TASKS))
                machines = {m for _v, m, _p in batch if m != "?"}
                self.assertEqual([v for v, _m, _p in batch], list(range(_N_TASKS)))
                saw_new_machine = "machine-1" in machines

        print(f"provisioning new machine: machine-1 picked up work = {saw_new_machine}")
        self.assertTrue(saw_new_machine, "the newly provisioned machine did not pick up any work")


if __name__ == "__main__":
    unittest.main()
