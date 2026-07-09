"""Container-scaling end-to-end: real workers in real containers, no cloud and no AWS mock.

A real client bursts work at a real scheduler; the scheduler's scaling policy drives one (or more)
``ContainerWorkerProvisioner``s to launch container "machines" -- each a fixed ``baremetal_native``
worker manager with its own IP -- which run the tasks and return real results. Correct results are the
proof that real containerized workers ran the work; the running-container count is the proof of how the
pool scaled.

Two classes:
  * ``TestContainerScalingE2E`` -- one auto-scaling manager: burst scale-up, scale-down / drain-to-zero,
    spread across machines, and a machine added mid-flight under rising load.
  * ``TestContainerWaterfallE2E`` -- two managers at different waterfall priorities: work fills the
    high-priority manager first and spills onto the low-priority one when it saturates.

Opt-in (``RUN_CONTAINER_E2E=1``) and Docker-only; machines run the self-contained worker image built by
``_container_image`` (the exact version/protocol the scheduler speaks). The harness also starts the web
GUI wired to the scheduler monitor and prints its URL, so a local run can be watched in a browser.
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
import unittest
from multiprocessing import get_context

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from tests.integration import CONTAINER_E2E_SKIP_REASON, RUN_CONTAINER_E2E
from tests.integration._container_backend import run_container_worker_manager
from tests.integration._container_image import ensure_worker_image
from tests.integration._container_runtime import DockerRuntime
from tests.integration._harness import SchedulerHarness
from tests.integration._tasks import square, square_on_machine
from tests.utility.utility import logging_test_name, terminate_process

_CLI = os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()
_REBUILD = os.environ.get("SCALER_IT_REBUILD") == "1"

_WORKERS_PER_MACHINE = 2
_MAX_MACHINES = 3
_MACHINE_PREFIX = "scaler-it-machine"
# Every provisioner names its containers "scaler-it-*", so this catches them all for last-resort cleanup.
_ALL_MACHINES_FILTER = "scaler-it"

_POLL_SECONDS = 0.5
# A burst deep enough that the scaling policy targets more than one machine's worth of concurrency.
_SPREAD_TASKS = 24
_SPREAD_TIMEOUT_SECONDS = 120.0
# A load held in flight while we sample the peak running-machine count, then let go idle.
_HOLD_LOAD_TASKS = 30
_SCALE_UP_OBSERVE_SECONDS = 20.0
_DRAIN_TIMEOUT_SECONDS = 90.0
# A concurrency-1 trickle needs only one machine at a time; a heavier burst must provision more.
_WARMUP_TASKS = 4

# Hold the steady load long enough that churn accumulates a decisive machine count (each flap creates
# another), well clear of the small re-provision slack.
_CHURN_LOAD_SECONDS = 45.0
_CHURN_SAMPLE_SECONDS = 0.4
_CHURN_TOLERANCE = 2

# Waterfall: manager A (priority 1) caps at one machine's worth; overflow spills to manager B (priority 2).
_MANAGER_A = "wm-container-a"
_MANAGER_B = "wm-container-b"
_PREFIX_A = "scaler-it-a"
_PREFIX_B = "scaler-it-b"
_MANAGER_A_CAP = _WORKERS_PER_MACHINE
# Waterfall scales up at tasks/workers > 10, so the backlog must clear A's cap by a wide margin to spill.
_WATERFALL_TASKS = 60
_WATERFALL_SPILL_TIMEOUT_SECONDS = 120.0

_RUN_CONTAINER_E2E = RUN_CONTAINER_E2E and DockerRuntime.is_available()
_SKIP_REASON = (
    CONTAINER_E2E_SKIP_REASON if not RUN_CONTAINER_E2E else "Docker is required for the container-scaling e2e"
)


def _running_machines(prefix: str) -> int:
    result = subprocess.run([*_CLI, "ps", "-q", "--filter", f"name={prefix}"], capture_output=True, text=True)
    return len(result.stdout.split())


def _running_machine_names(prefix: str) -> list:
    result = subprocess.run(
        [*_CLI, "ps", "--filter", f"name={prefix}", "--format", "{{.Names}}"], capture_output=True, text=True
    )
    return [name for name in result.stdout.split() if name]


def _max_machine_number(names: list) -> int:
    """The highest trailing counter across ``prefix-N`` container names. The provisioner numbers machines
    monotonically, so the max ever seen is how many were created -- a churn-robust total that a single
    sample of the running set (which misses machines that already came and went) would undercount."""
    best = 0
    for name in names:
        try:
            best = max(best, int(name.rsplit("-", 1)[-1]))
        except ValueError:
            continue
    return best


def _remove_machines(prefix: str) -> None:
    ids = subprocess.run(
        [*_CLI, "ps", "-aq", "--filter", f"name={prefix}"], capture_output=True, text=True
    ).stdout.split()
    for container in ids:
        subprocess.run([*_CLI, "rm", "-f", container], capture_output=True)


@unittest.skipUnless(_RUN_CONTAINER_E2E, _SKIP_REASON)
class TestContainerScalingE2E(unittest.TestCase):
    """One auto-scaling container manager against a scheduler that starts with zero machines."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_worker_image(rebuild=_REBUILD)

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.runtime = DockerRuntime()
        self.addCleanup(_remove_machines, _ALL_MACHINES_FILTER)  # last resort: reap any leftover container
        self.harness = SchedulerHarness(gateway=self.runtime.host_gateway(), enable_webgui=True)
        self.addCleanup(self.harness.shutdown)
        self.manager = get_context("spawn").Process(
            target=run_container_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                _WORKERS_PER_MACHINE,
                _MAX_MACHINES,
                0.0,
                0.0,
                "wm-container-it",
                _MACHINE_PREFIX,
            ),
        )
        self.manager.start()
        self.addCleanup(terminate_process, self.manager)

    def test_burst_scales_up_container_machines_and_runs_tasks(self) -> None:
        """From zero machines, a burst forces the scheduler to provision container machines that compute
        the results -- correct results are themselves proof the containers ran the work."""
        inputs = list(range(_SPREAD_TASKS))
        with Client(self.harness.scheduler_address) as client:
            results = client.map(square, inputs)
        self.assertEqual(results, [value * value for value in inputs])

    def test_scale_down_and_drain_to_zero_when_idle(self) -> None:
        """After a burst brings machines up, an idle queue must tear every machine back down (the scaling
        policy targets desired=0 on an empty queue). Machines use --rm, so a stopped machine simply
        disappears -- the running-container count draining to zero is the proof."""
        with Client(self.harness.scheduler_address) as client:
            futures = [client.submit(square, value) for value in range(_HOLD_LOAD_TASKS)]
            peak = 0
            deadline = time.monotonic() + _SCALE_UP_OBSERVE_SECONDS
            while time.monotonic() < deadline:
                peak = max(peak, _running_machines(_MACHINE_PREFIX))
                time.sleep(_POLL_SECONDS)
            results = [future.result() for future in futures]
        self.assertEqual(results, [value * value for value in range(_HOLD_LOAD_TASKS)])
        self.assertGreaterEqual(peak, 1, "no container machine came up under load")

        deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
        while time.monotonic() < deadline and _running_machines(_MACHINE_PREFIX) > 0:
            time.sleep(1.0)
        drained = _running_machines(_MACHINE_PREFIX)
        print(f"container scaling: peak {peak} machine(s) -> {drained} when idle")
        self.assertEqual(drained, 0, f"machines did not drain to zero when idle (still {drained} running)")

    def test_work_spreads_across_multiple_machines(self) -> None:
        """A burst deep enough to need more than one machine's concurrency must run work on >= 2 distinct
        machines (each container tags its workers with its own SCALER_IT_MACHINE_ID). Machines churn under
        the vanilla policy's aggressive scale-down, so this counts distinct machines used over the run."""
        machines: set = set()
        with Client(self.harness.scheduler_address) as client:
            deadline = time.monotonic() + _SPREAD_TIMEOUT_SECONDS
            while len(machines) < 2 and time.monotonic() < deadline:
                results = client.map(square_on_machine, range(_SPREAD_TASKS))
                self.assertEqual(
                    [value for value, _machine in results], [value * value for value in range(_SPREAD_TASKS)]
                )
                machines |= {machine for _value, machine in results if machine != "?"}
        print(f"container scaling: work ran across machines {sorted(machines)}")
        self.assertGreaterEqual(len(machines), 2, f"work only ran on {machines}; expected >= 2 machines")

    def test_rising_load_provisions_more_machines(self) -> None:
        """Capacity tracks demand: a concurrency-1 trickle needs only a single machine at a time, while a
        heavier burst must provision additional machines. Machines churn under the vanilla policy's
        aggressive scale-down, so this compares the distinct machines used by each phase rather than a
        concurrent snapshot."""
        with Client(self.harness.scheduler_address) as client:
            trickle_machines: set = set()
            for value in range(_WARMUP_TASKS):
                _result, machine = client.submit(square_on_machine, value).result()
                if machine != "?":
                    trickle_machines.add(machine)

            burst_machines: set = set()
            deadline = time.monotonic() + _SPREAD_TIMEOUT_SECONDS
            while len(burst_machines) <= len(trickle_machines) and time.monotonic() < deadline:
                batch = client.map(square_on_machine, range(_SPREAD_TASKS))
                self.assertEqual([value for value, _m in batch], [value * value for value in range(_SPREAD_TASKS)])
                burst_machines |= {machine for _value, machine in batch if machine != "?"}

        print(f"container scaling: trickle used {len(trickle_machines)} machine(s), burst used {len(burst_machines)}")
        self.assertGreater(
            len(burst_machines),
            len(trickle_machines),
            "rising load did not provision more machines than a concurrency-1 trickle",
        )

    def test_steady_load_uses_a_stable_pool_not_churn(self) -> None:
        """Churn tripwire (steady-load only): a sustained, non-varying load should hold a stable pool --
        about as many machines CREATED as ever run CONCURRENTLY at the peak. Because boot latency far
        exceeds the task time, the vanilla policy flaps and cycles through many more machines than ever run
        at once (workers disconnect, tasks retry) until the scale-down cooldown (inherited from the shared
        CapacityCoordinator) damps it. Do NOT copy this onto tests that scale up AND down -- those
        legitimately create more machines than the peak."""
        stop = threading.Event()
        peak = 0
        created = 0

        def sample() -> None:
            nonlocal peak, created
            while not stop.is_set():
                names = _running_machine_names(_MACHINE_PREFIX)
                peak = max(peak, len(names))
                created = max(created, _max_machine_number(names))
                time.sleep(_CHURN_SAMPLE_SECONDS)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            with Client(self.harness.scheduler_address) as client:
                deadline = time.monotonic() + _CHURN_LOAD_SECONDS
                while time.monotonic() < deadline:
                    client.map(square, range(_SPREAD_TASKS))  # steady, back-to-back waves of load
        finally:
            stop.set()
            sampler.join(timeout=5.0)

        print(f"container scaling churn: created {created} machines, peak {peak} concurrent")
        self.assertGreaterEqual(created, 1, "no machines were provisioned under the steady load")
        self.assertLessEqual(
            created,
            peak + _CHURN_TOLERANCE,
            f"scaling churned: created {created} machines but only {peak} ran concurrently at the peak "
            f"(machines flapping under steady load; the scale-down cooldown should damp this)",
        )


@unittest.skipUnless(_RUN_CONTAINER_E2E, _SKIP_REASON)
class TestContainerWaterfallE2E(unittest.TestCase):
    """Two container managers at different waterfall priorities on one scheduler: work fills the
    high-priority manager first and spills onto the low-priority one when it saturates."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_worker_image(rebuild=_REBUILD)

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.runtime = DockerRuntime()
        self.addCleanup(_remove_machines, _ALL_MACHINES_FILTER)
        rules = f"1,{_MANAGER_A},{_MANAGER_A_CAP}\n2,{_MANAGER_B}"
        self.harness = SchedulerHarness(
            policy_content=rules,
            policy_engine_type="waterfall_v1",
            gateway=self.runtime.host_gateway(),
            enable_webgui=True,
        )
        self.addCleanup(self.harness.shutdown)

    def _start_manager(self, worker_manager_id: str, name_prefix: str, max_machines: int):
        process = get_context("spawn").Process(
            target=run_container_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                _WORKERS_PER_MACHINE,
                max_machines,
                0.0,
                0.0,
                worker_manager_id,
                name_prefix,
            ),
        )
        process.start()
        self.addCleanup(terminate_process, process)
        return process

    def test_waterfall_spills_to_lower_priority_manager(self) -> None:
        # Priority-1 manager A alone (capped at one machine); a burst lands entirely on A.
        self._start_manager(_MANAGER_A, _PREFIX_A, max_machines=1)
        with Client(self.harness.scheduler_address) as client:
            warmup = client.map(square_on_machine, range(_WATERFALL_TASKS))
            self.assertEqual(
                [value for value, _machine in warmup], [value * value for value in range(_WATERFALL_TASKS)]
            )
            on_a = {machine for _value, machine in warmup if machine != "?"}
            self.assertTrue(
                on_a and all(machine.startswith(_PREFIX_A) for machine in on_a),
                f"expected the warmup burst to run only on manager A, saw {on_a}",
            )

            # Bring up priority-2 manager B; sustained overflow beyond A's cap must spill onto B.
            self._start_manager(_MANAGER_B, _PREFIX_B, max_machines=2)
            saw_b = False
            deadline = time.monotonic() + _WATERFALL_SPILL_TIMEOUT_SECONDS
            while not saw_b and time.monotonic() < deadline:
                batch = client.map(square_on_machine, range(_WATERFALL_TASKS))
                self.assertEqual([value for value, _m in batch], [value * value for value in range(_WATERFALL_TASKS)])
                saw_b = any(machine.startswith(_PREFIX_B) for _value, machine in batch if machine != "?")

        print(f"container waterfall: work spilled onto lower-priority manager B = {saw_b}")
        self.assertTrue(saw_b, "work never spilled onto the lower-priority manager B")


if __name__ == "__main__":
    unittest.main()
