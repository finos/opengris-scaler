"""ECS scaling end-to-end: the SHIPPED ECS worker manager driven through a real scale curve on floci.

Where the control-plane test (``test_ecs_provisioning.py``) asserts that ``ECSWorkerProvisioner`` calls
AWS correctly against a mock that never boots anything, this runs the *same shipped code* against a floci
emulator that actually launches each ``RunTask`` as a sibling Docker container -- so real workers connect
back, run real tasks, and return real results. Nothing about the worker manager is faked: only boto3's
endpoint is redirected to floci.

A real client bursts work at a real scheduler; the scheduler's scaling policy drives the real
``ECSWorkerManager`` to launch ECS task containers; correct results prove those containers ran the work and
the running-container count proves how the pool scaled. Tasks are submitted BY VALUE (nested functions,
cloudpickled by value) because the shipped provisioner mounts no repo into the task, and each task tags its
work by container hostname since the provisioner sets no machine id.

Opt-in (``RUN_FLOCI_E2E=1``) and Docker-only; the task image is built by ``_container_image`` and floci is
managed by ``_floci``. The harness also starts the web GUI wired to the scheduler monitor and prints its
URL, so a local run can be watched in a browser.
"""

from __future__ import annotations

import os
import threading
import time
import unittest
from multiprocessing import get_context

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from tests.integration import FLOCI_E2E_SKIP_REASON, RUN_FLOCI_E2E
from tests.integration._container_image import DEFAULT_ECS_IMAGE_TAG, ensure_ecs_worker_image
from tests.integration._container_runtime import DockerRuntime
from tests.integration._ecs_backend import run_ecs_worker_manager
from tests.integration._floci import (
    FlociEmulator,
    floci_available,
    remove_task_containers,
    running_task_container_names,
    running_task_containers,
)
from tests.integration._harness import SchedulerHarness, assert_backend_processes_alive
from tests.utility.utility import logging_test_name, terminate_process

_REBUILD = os.environ.get("SCALER_IT_REBUILD") == "1"

# ecs_task_cpu = workers per ECS task and the scale-up divisor; the cap allows up to three tasks
# (ceil(6 / 2)), enough to observe spread without oversubscribing the host.
_ECS_TASK_CPU = 2
_MAX_TASK_CONCURRENCY = 6

# Non-instant tasks keep the load sustained so the pool stays up long enough to observe; light enough that
# a burst oversubscribes a couple of tasks' worth of workers.
_TASK_SECONDS = 0.15
# A burst deep enough that the scaling policy targets more than one ECS task's worth of concurrency.
_BURST_TASKS = 24

_POLL_SECONDS = 0.5
# A held load kept in flight while we sample the peak running-task count, then released to idle.
_HELD_LOAD_TASKS = 30
_SCALE_UP_OBSERVE_SECONDS = 30.0
_DRAIN_TIMEOUT_SECONDS = 90.0
_SPREAD_TIMEOUT_SECONDS = 150.0
# A concurrency-1 trickle needs only one task at a time; a heavier burst must provision more.
_WARMUP_TASKS = 4

# Steady-load stability: hold a steady load and check the pool settles rather than thrashes (creating many
# more task containers than ever run at once). Tolerance leaves room for the mild create>peak gap a real
# manager shows while still catching a decisively churning pool.
_CHURN_LOAD_SECONDS = 45.0
_CHURN_SAMPLE_SECONDS = 0.4
_CHURN_TOLERANCE = 2


def _make_tasks():
    """Return ``(square, square_on_host)`` as nested functions so cloudpickle serializes them BY VALUE.

    The shipped ECS provisioner mounts no repo into the task container, so a module-level (by-reference)
    task would fail to import there; a nested function is pickled whole and needs nothing but the stdlib.
    ``square_on_host`` also reports its container hostname so a test can see work spread across tasks."""

    def square(value: int) -> int:
        import time

        time.sleep(_TASK_SECONDS)
        return value * value

    def square_on_host(value: int):
        import socket
        import time

        time.sleep(_TASK_SECONDS)
        return value * value, socket.gethostname()

    return square, square_on_host


_RUN_FLOCI_E2E = RUN_FLOCI_E2E and floci_available()
_SKIP_REASON = FLOCI_E2E_SKIP_REASON if not RUN_FLOCI_E2E else "Docker is required for the floci-backed ECS e2e"


@unittest.skipUnless(_RUN_FLOCI_E2E, _SKIP_REASON)
class TestECSScalingE2E(unittest.TestCase):
    """The shipped ECS worker manager, provisioning real task containers on floci from zero."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_ecs_worker_image(rebuild=_REBUILD)

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.runtime = DockerRuntime()
        self.addCleanup(remove_task_containers)  # last resort: reap any leftover ECS task container

        self.floci = FlociEmulator().start()
        self.addCleanup(self.floci.stop)

        self.harness = SchedulerHarness(gateway=self.runtime.host_gateway(), enable_webgui=True)
        self.addCleanup(self.harness.shutdown)

        self.manager = get_context("spawn").Process(
            target=run_ecs_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                self.floci.endpoint_url,
                DEFAULT_ECS_IMAGE_TAG,
                _ECS_TASK_CPU,
                _MAX_TASK_CONCURRENCY,
            ),
        )
        self.manager.start()
        self.addCleanup(terminate_process, self.manager)

    def tearDown(self) -> None:
        # Runs before the addCleanup teardown, so the scheduler/manager are still in their post-test state:
        # if either crashed under churn, fail with that instead of the client's downstream TimeoutError.
        assert_backend_processes_alive(self, self.harness, worker_manager=self.manager)

    def test_burst_scales_up_ecs_tasks_then_drains_to_zero(self) -> None:
        """From zero tasks, a held burst forces the shipped manager to launch ECS task containers that
        compute the results (correct results prove the containers ran the work); once the queue goes idle
        the scaling policy tears every task back down (running-container count draining to zero)."""
        square, _ = _make_tasks()
        with Client(self.harness.scheduler_address) as client:
            futures = [client.submit(square, value) for value in range(_HELD_LOAD_TASKS)]
            peak = 0
            deadline = time.monotonic() + _SCALE_UP_OBSERVE_SECONDS
            while time.monotonic() < deadline:
                peak = max(peak, running_task_containers())
                time.sleep(_POLL_SECONDS)
            results = [future.result() for future in futures]

        self.assertEqual(results, [value * value for value in range(_HELD_LOAD_TASKS)])
        self.assertGreaterEqual(peak, 1, "no ECS task container came up under load")

        deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
        while time.monotonic() < deadline and running_task_containers() > 0:
            time.sleep(1.0)
        drained = running_task_containers()
        print(f"ecs scaling: peak {peak} task container(s) -> {drained} when idle")
        self.assertEqual(drained, 0, f"ECS tasks did not drain to zero when idle (still {drained} running)")

    def test_work_spreads_across_multiple_ecs_tasks(self) -> None:
        """A burst deep enough to need more than one task's concurrency must run work on >= 2 distinct task
        containers (each tags its work with its own hostname). Tasks churn under the vanilla policy's
        aggressive scale-down, so this counts distinct hosts used over the run, not a concurrent snapshot."""
        _, square_on_host = _make_tasks()
        hosts: set = set()
        with Client(self.harness.scheduler_address) as client:
            deadline = time.monotonic() + _SPREAD_TIMEOUT_SECONDS
            while len(hosts) < 2 and time.monotonic() < deadline:
                results = client.map(square_on_host, range(_BURST_TASKS))
                self.assertEqual([value for value, _host in results], [value * value for value in range(_BURST_TASKS)])
                hosts |= {host for _value, host in results if host}
        print(f"ecs scaling: work ran across task hosts {sorted(hosts)}")
        self.assertGreaterEqual(len(hosts), 2, f"work only ran on {hosts}; expected >= 2 ECS task containers")

    def test_rising_load_provisions_more_ecs_tasks(self) -> None:
        """Capacity tracks demand: a concurrency-1 trickle needs only a single task at a time, while a
        heavier burst must provision additional tasks. Tasks churn under the vanilla policy's aggressive
        scale-down, so this compares the distinct hosts used by each phase, not a concurrent snapshot."""
        _, square_on_host = _make_tasks()
        with Client(self.harness.scheduler_address) as client:
            trickle_hosts: set = set()
            for value in range(_WARMUP_TASKS):
                _result, host = client.submit(square_on_host, value).result()
                if host:
                    trickle_hosts.add(host)

            burst_hosts: set = set()
            deadline = time.monotonic() + _SPREAD_TIMEOUT_SECONDS
            while len(burst_hosts) <= len(trickle_hosts) and time.monotonic() < deadline:
                batch = client.map(square_on_host, range(_BURST_TASKS))
                self.assertEqual([value for value, _host in batch], [value * value for value in range(_BURST_TASKS)])
                burst_hosts |= {host for _value, host in batch if host}

        print(f"ecs scaling: trickle used {len(trickle_hosts)} host(s), burst used {len(burst_hosts)}")
        self.assertGreater(
            len(burst_hosts),
            len(trickle_hosts),
            "rising load did not provision more ECS tasks than a concurrency-1 trickle",
        )

    def test_steady_load_uses_a_stable_pool_not_churn(self) -> None:
        """Steady-load stability (steady-load only): a sustained, non-varying load should settle on a stable
        pool -- about as many task containers CREATED over the run as ever run CONCURRENTLY at the peak. A
        pool that instead cycles through many more units than it ever runs at once is thrashing under the
        provision/teardown loop (workers disconnect mid-flight, tasks retry). Steady-load only: do NOT copy
        onto tests that scale up AND down, which legitimately create more units than the peak."""
        square, _ = _make_tasks()
        stop = threading.Event()
        peak = 0
        created: set = set()

        def sample() -> None:
            nonlocal peak
            while not stop.is_set():
                names = running_task_container_names()
                peak = max(peak, len(names))
                created.update(names)
                time.sleep(_CHURN_SAMPLE_SECONDS)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            with Client(self.harness.scheduler_address) as client:
                deadline = time.monotonic() + _CHURN_LOAD_SECONDS
                while time.monotonic() < deadline:
                    client.map(square, range(_BURST_TASKS))  # steady, back-to-back waves of load
        finally:
            stop.set()
            sampler.join(timeout=5.0)

        total_created = len(created)
        print(f"ecs scaling churn: created {total_created} task container(s), peak {peak} concurrent")
        self.assertGreaterEqual(total_created, 1, "no ECS task container was provisioned under the steady load")
        self.assertLessEqual(
            total_created,
            peak + _CHURN_TOLERANCE,
            f"pool thrashed: created {total_created} task containers but only {peak} ran concurrently at the "
            f"peak (steady load should settle on a stable pool, not repeatedly provision and tear down)",
        )


if __name__ == "__main__":
    unittest.main()
