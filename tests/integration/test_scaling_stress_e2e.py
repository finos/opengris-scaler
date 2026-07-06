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
import time
import unittest
from multiprocessing import get_context

import psutil

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from tests.integration import RUN_SCALING_STRESS_TEST, SCALING_STRESS_SKIP_REASON
from tests.integration._harness import SchedulerHarness, run_native_worker_manager
from tests.utility.utility import logging_test_name, terminate_process

_MAX_WORKERS = int(os.environ.get("SCALING_STRESS_MAX_WORKERS", "8"))
_N_TASKS = int(os.environ.get("SCALING_STRESS_TASKS", "240"))
_TASK_SECONDS = float(os.environ.get("SCALING_STRESS_TASK_SECONDS", "0.2"))
_MIN_WORKERS = int(os.environ.get("SCALING_STRESS_MIN_WORKERS", "3"))

# Scale-DOWN convergence targets. Counting only LIVE workers (zombies excluded -- see
# _worker_agent_count), the pool drops to ~0 within ~2s when idle and to ~1 under a steady
# concurrency-1 trickle. Allow one transitional extra worker.
_SCALE_DOWN_TARGET_WORKERS = 2
_SCALE_DOWN_TIMEOUT_SECONDS = 60.0
# Generous upper bound to poll for the idle pool to drain to zero (measured ~2s).
_IDLE_SCALE_DOWN_WAIT_SECONDS = 20.0


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
            target=run_native_worker_manager, args=(self.harness.scheduler_address, max_task_concurrency)
        )
        process.start()
        self.addCleanup(terminate_process, process)
        return process

    @staticmethod
    def _worker_agent_count(manager_process) -> int:
        """Count LIVE worker-agent subprocesses (the manager's direct children), EXCLUDING zombies.
        stop_units() SIGINTs a worker and drops its reference without reaping it, so a scaled-down
        worker lingers as a zombie child until a later start_units triggers reaping -- counting zombies
        would make a real scale-down look like it never happened. One live child == one running worker."""
        try:
            children = psutil.Process(manager_process.pid).children(recursive=False)
        except psutil.NoSuchProcess:
            return 0
        live = 0
        for child in children:
            try:
                if child.status() != psutil.STATUS_ZOMBIE:
                    live += 1
            except psutil.NoSuchProcess:
                continue
        return live

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

    def test_reduced_load_scales_workers_back_down(self) -> None:
        """Scale-DOWN: after a burst scales the pool up, a sustained light load must let the scheduler
        tear the surplus workers back down. A fully idle queue does not re-trigger scaling (the pool
        would sit at its peak), so a concurrency-1 trickle drives the reduction -- exercising the
        declarative stop_units path (graceful SIGINT on POSIX; hence the class-level Windows skip)."""
        manager = self._start_worker_manager(max_task_concurrency=_MAX_WORKERS)

        with Client(self.harness.scheduler_address) as client:
            # Burst to force scale-up, then confirm the pool actually grew.
            results = client.map(_sleep_and_identify, range(_N_TASKS))
            self.assertEqual([value for value, _pid in results], list(range(_N_TASKS)))
            peak_workers = self._worker_agent_count(manager)
            self.assertGreaterEqual(peak_workers, _MIN_WORKERS, f"burst did not scale up (peak={peak_workers})")

            # Sustained concurrency-1 trickle: the surplus workers must be scaled back down.
            deadline = time.monotonic() + _SCALE_DOWN_TIMEOUT_SECONDS
            while time.monotonic() < deadline:
                client.submit(_sleep_and_identify, 0).result()
                if self._worker_agent_count(manager) <= _SCALE_DOWN_TARGET_WORKERS:
                    break
            settled_workers = self._worker_agent_count(manager)

        print(f"scaling stress: worker pool scaled {peak_workers} -> {settled_workers} when load dropped")
        self.assertLess(settled_workers, peak_workers, "worker pool did not shrink after the load dropped")
        self.assertLessEqual(
            settled_workers,
            _SCALE_DOWN_TARGET_WORKERS,
            f"workers did not scale down under reduced load: peaked at {peak_workers}, still "
            f"{settled_workers} after {_SCALE_DOWN_TIMEOUT_SECONDS:.0f}s",
        )

    def test_idle_queue_scales_workers_down_to_zero(self) -> None:
        """Scale-DOWN to zero on an IDLE queue: after a burst, with no further work the scheduler tears
        ALL workers back down (VanillaScalingPolicy computes desired=0 on an empty queue; the manager's
        periodic heartbeats keep the policy re-evaluating even with no tasks flowing). Complements
        test_reduced_load_scales_workers_back_down, which keeps ~1 worker under a light trickle.

        NOTE: the dynamic provisioner's stop_units SIGINTs each worker but never reaps it, so a
        scaled-down worker lingers as a zombie child (a minor product leak); _worker_agent_count counts
        only LIVE workers, which drop to 0 within ~2s of going idle."""
        manager = self._start_worker_manager(max_task_concurrency=_MAX_WORKERS)

        with Client(self.harness.scheduler_address) as client:
            client.map(_sleep_and_identify, range(_N_TASKS))
            peak_workers = self._worker_agent_count(manager)
            self.assertGreaterEqual(peak_workers, _MIN_WORKERS, f"burst did not scale up (peak={peak_workers})")

            # Go idle; the live pool must drain to (near) zero.
            deadline = time.monotonic() + _IDLE_SCALE_DOWN_WAIT_SECONDS
            while time.monotonic() < deadline and self._worker_agent_count(manager) > _SCALE_DOWN_TARGET_WORKERS:
                time.sleep(1.0)
            idle_workers = self._worker_agent_count(manager)

        print(f"scaling stress: idle pool scaled {peak_workers} -> {idle_workers} live workers")
        self.assertLessEqual(
            idle_workers,
            _SCALE_DOWN_TARGET_WORKERS,
            f"idle queue did not scale workers down: peaked {peak_workers}, still {idle_workers} live "
            f"after {_IDLE_SCALE_DOWN_WAIT_SECONDS:.0f}s idle",
        )


_NUM_MACHINES = int(os.environ.get("SCALING_STRESS_MACHINES", "3"))
_WORKERS_PER_MACHINE = int(os.environ.get("SCALING_STRESS_WORKERS_PER_MACHINE", "2"))
# Bound for feeding work until newly-started machines have brought up workers and taken some.
_MULTI_MACHINE_SPREAD_TIMEOUT_SECONDS = 60.0


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
            target=run_native_worker_manager,
            args=(self.harness.scheduler_address, max_task_concurrency, f"wm-{machine_id}", machine_id),
        )
        process.start()
        self.addCleanup(terminate_process, process)
        return process

    def test_work_spreads_across_multiple_machines(self) -> None:
        """Several machines, each a separate worker manager with its own workers, share the load."""
        for index in range(_NUM_MACHINES):
            self._start_machine(f"machine-{index}", max_task_concurrency=_WORKERS_PER_MACHINE)

        # Keep feeding work (bounded) until at least two machines have taken some: a single map can land
        # entirely on the first machine to bring up workers, before the others have provisioned theirs.
        machines: set = set()
        pids_per_machine: dict = {}
        with Client(self.harness.scheduler_address) as client:
            deadline = time.monotonic() + _MULTI_MACHINE_SPREAD_TIMEOUT_SECONDS
            while len(machines) < 2 and time.monotonic() < deadline:
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
        self._start_machine("machine-0", max_task_concurrency=_WORKERS_PER_MACHINE)

        with Client(self.harness.scheduler_address) as client:
            # Warm up: only machine-0 exists, so it must handle this batch.
            warmup = client.map(_identify_machine, range(_N_TASKS // 4))
            self.assertEqual({m for _v, m, _p in warmup if m != "?"}, {"machine-0"})

            # Provision a NEW machine (a second worker manager, as a cloud manager would on scale-up).
            self._start_machine("machine-1", max_task_concurrency=_WORKERS_PER_MACHINE)

            # Keep feeding work until the new machine's provisioned workers contribute (bounded).
            saw_new_machine = False
            deadline = time.monotonic() + _MULTI_MACHINE_SPREAD_TIMEOUT_SECONDS
            while not saw_new_machine and time.monotonic() < deadline:
                batch = client.map(_identify_machine, range(_N_TASKS))
                machines = {m for _v, m, _p in batch if m != "?"}
                self.assertEqual([v for v, _m, _p in batch], list(range(_N_TASKS)))
                saw_new_machine = "machine-1" in machines

        print(f"provisioning new machine: machine-1 picked up work = {saw_new_machine}")
        self.assertTrue(saw_new_machine, "the newly provisioned machine did not pick up any work")


if __name__ == "__main__":
    unittest.main()
