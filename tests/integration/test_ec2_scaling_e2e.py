"""EC2 scaling end-to-end: the SHIPPED ORB/EC2 worker manager driven through a real scale curve on floci.

Where the control-plane test (``test_ec2_orb_provisioning.py``) asserts that the ORB provisioner calls AWS
correctly against a mock that never boots anything, this runs the *same shipped code* against a floci
emulator that actually launches each ``RunInstances`` as a real Amazon Linux 2023 container. The shipped
UserData bootstrap then installs a ``manylinux`` wheel of the CURRENT source -- built by cibuildwheel and
served over the docker-bridge gateway -- and boots a worker that connects back, runs work, and returns
results. Nothing about the worker manager is faked: only boto3's endpoint and the requirements URL differ.

A real client bursts work at a real scheduler; the scheduler's scaling policy drives the real
``ORBAWSEC2WorkerManager`` to launch instances; correct results prove those instances ran the work and the
running-container count proves how the pool scaled. Tasks are submitted BY VALUE (nested functions,
cloudpickled whole) because the instance has only the wheel, not the test module; each task tags its work
by instance hostname since the provisioner sets no machine id.

Opt-in (``RUN_EC2_E2E=1``) and Docker-only, and it needs a prebuilt manylinux wheel of the current source
(``scripts/build_cibuildwheel.sh``); it skips itself if that wheel is absent. Boots are real installs, so
timeouts are generous. The harness also starts the web GUI wired to the scheduler monitor.
"""

from __future__ import annotations

import os
import sys
import threading
import time
import unittest
from multiprocessing import get_context

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.integration import EC2_E2E_SKIP_REASON, RUN_EC2_E2E
from tests.integration._container_image import ensure_ec2_base_image
from tests.integration._container_runtime import DockerRuntime
from tests.integration._ec2_backend import (
    WHEEL_DIR,
    manylinux_wheel,
    run_ec2_worker_manager,
    serve_directory_on_gateway,
)
from tests.integration._floci import (
    FLOCI_INSTANCE_CONTAINER_PREFIX,
    FlociEmulator,
    floci_available,
    remove_task_containers,
    running_task_container_names,
    running_task_containers,
)
from tests.integration._harness import SchedulerHarness, assert_backend_processes_alive
from tests.utility.utility import logging_test_name, terminate_process

_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"

# t3.medium reports 2 vCPUs; the cap allows up to two instances (ceil(4 / 2)).
_MAX_TASK_CONCURRENCY = 4
_BURST_TASKS = 24
_TASK_SECONDS = 0.15

_POLL_SECONDS = 1.0
# An EC2 "boot" is a real install (dnf + uv + pip of the wheel), so allow minutes, not seconds.
_PROVISION_TIMEOUT_SECONDS = 300.0
_DRAIN_TIMEOUT_SECONDS = 180.0
_SPREAD_TIMEOUT_SECONDS = 420.0
# Minute-plus boots stress the scheduler while a pool churns, stalling the client heartbeat well past the
# stock 60s; size the client/worker liveness to the provisioning latency so a healthy run is not cut off.
_CLIENT_TIMEOUT_SECONDS = 300
_WORKER_TIMEOUT_SECONDS = 120
# A concurrency-1 trickle needs only one instance at a time; a heavier burst must provision more.
_WARMUP_TASKS = 4

# Steady-load stability: hold a steady load for a few minutes and check the pool settles rather than thrashes
# (creating many more instances than ever run at once). Tolerance leaves room for the mild create>peak gap a
# real cloud manager shows while still catching a decisively churning pool.
_CHURN_LOAD_SECONDS = 180.0
_CHURN_SAMPLE_SECONDS = 1.0
_CHURN_TOLERANCE = 3


def _make_tasks():
    """Return ``(square, square_on_host)`` as nested functions so cloudpickle serializes them BY VALUE (the
    instance has only the scaler wheel, not the test module); ``square_on_host`` also reports its instance
    hostname so a test can see work spread across instances."""

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


_WHEEL = manylinux_wheel()
_RUN_EC2_E2E = RUN_EC2_E2E and floci_available() and bool(_WHEEL)
if not RUN_EC2_E2E:
    _SKIP_REASON = EC2_E2E_SKIP_REASON
elif not floci_available():
    _SKIP_REASON = "Docker is required for the floci-backed EC2 e2e"
else:
    _SKIP_REASON = f"no manylinux wheel under {WHEEL_DIR}; build one with scripts/build_cibuildwheel.sh"


@unittest.skipUnless(_RUN_EC2_E2E, _SKIP_REASON)
class TestEC2ScalingE2E(unittest.TestCase):
    """The shipped ORB/EC2 worker manager, provisioning real AL2023 instances on floci from zero."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_ec2_base_image()

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.runtime = DockerRuntime()
        gateway = self.runtime.host_gateway()
        self.addCleanup(remove_task_containers, FLOCI_INSTANCE_CONTAINER_PREFIX)

        self.floci = FlociEmulator().start()
        self.addCleanup(self.floci.stop)

        wheel_port = get_available_tcp_port()
        self._wheel_server = serve_directory_on_gateway(WHEEL_DIR, "0.0.0.0", wheel_port)
        self.addCleanup(self._wheel_server.shutdown)
        wheel_url = f"http://{gateway}:{wheel_port}/{os.path.basename(_WHEEL)}"

        self.harness = SchedulerHarness(
            gateway=gateway,
            enable_webgui=True,
            client_timeout_seconds=_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=_WORKER_TIMEOUT_SECONDS,
        )
        self.addCleanup(self.harness.shutdown)

        self.manager = get_context("spawn").Process(
            target=run_ec2_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                self.floci.endpoint_url,
                wheel_url,
                _PYTHON_VERSION,
                _MAX_TASK_CONCURRENCY,
            ),
        )
        self.manager.start()
        self.addCleanup(terminate_process, self.manager)

    def tearDown(self) -> None:
        # Runs before the addCleanup teardown, so the scheduler/manager are still in their post-test state:
        # if either crashed under churn, fail with that instead of the client's downstream TimeoutError.
        assert_backend_processes_alive(self, self.harness, worker_manager=self.manager)

    def _running_instances(self) -> int:
        return running_task_containers(FLOCI_INSTANCE_CONTAINER_PREFIX)

    def test_burst_provisions_an_instance_then_drains_to_zero(self) -> None:
        """From zero instances, a burst forces the shipped manager to launch an AL2023 instance whose
        UserData installs the current-source wheel and boots a worker that computes the results -- correct
        results prove the instance ran the work. When the queue goes idle the manager terminates the
        instance (running-container count draining to zero)."""
        square, _ = _make_tasks()
        with Client(self.harness.scheduler_address, timeout_seconds=_CLIENT_TIMEOUT_SECONDS) as client:
            futures = [client.submit(square, value) for value in range(_BURST_TASKS)]
            peak = 0
            deadline = time.monotonic() + _PROVISION_TIMEOUT_SECONDS
            while time.monotonic() < deadline and not all(future.done() for future in futures):
                peak = max(peak, self._running_instances())
                time.sleep(_POLL_SECONDS)
            results = [future.result() for future in futures]

        self.assertEqual(results, [value * value for value in range(_BURST_TASKS)])
        self.assertGreaterEqual(peak, 1, "no EC2 instance came up under load")

        deadline = time.monotonic() + _DRAIN_TIMEOUT_SECONDS
        while time.monotonic() < deadline and self._running_instances() > 0:
            time.sleep(_POLL_SECONDS)
        drained = self._running_instances()
        print(f"ec2 scaling: peak {peak} instance(s) -> {drained} when idle")
        self.assertEqual(drained, 0, f"EC2 instances did not drain to zero when idle (still {drained} running)")

    def test_work_spreads_across_multiple_instances(self) -> None:
        """A sustained load deep enough to need more than one instance's concurrency must run work on >= 2
        distinct instances (each tags its work with its own hostname). Instances take real time to boot, so
        this re-submits waves and counts distinct hosts used over the run."""
        _, square_on_host = _make_tasks()
        hosts: set = set()
        with Client(self.harness.scheduler_address, timeout_seconds=_CLIENT_TIMEOUT_SECONDS) as client:
            deadline = time.monotonic() + _SPREAD_TIMEOUT_SECONDS
            while len(hosts) < 2 and time.monotonic() < deadline:
                results = client.map(square_on_host, range(_BURST_TASKS))
                self.assertEqual([value for value, _host in results], [value * value for value in range(_BURST_TASKS)])
                hosts |= {host for _value, host in results if host}
        print(f"ec2 scaling: work ran across instance hosts {sorted(hosts)}")
        self.assertGreaterEqual(len(hosts), 2, f"work only ran on {hosts}; expected >= 2 EC2 instances")

    def test_rising_load_provisions_more_instances(self) -> None:
        """Capacity tracks demand: a concurrency-1 trickle needs only a single instance at a time, while a
        heavier burst must provision additional instances. Instances churn under the vanilla policy's
        aggressive scale-down, so this compares the distinct hosts used by each phase, not a snapshot."""
        _, square_on_host = _make_tasks()
        with Client(self.harness.scheduler_address, timeout_seconds=_CLIENT_TIMEOUT_SECONDS) as client:
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

        print(f"ec2 scaling: trickle used {len(trickle_hosts)} host(s), burst used {len(burst_hosts)}")
        self.assertGreater(
            len(burst_hosts),
            len(trickle_hosts),
            "rising load did not provision more EC2 instances than a concurrency-1 trickle",
        )

    def test_steady_load_uses_a_stable_pool_not_churn(self) -> None:
        """Steady-load stability (steady-load only): a sustained, non-varying load should settle on a stable
        pool -- about as many instances CREATED over the run as ever run CONCURRENTLY at the peak. A pool
        that instead cycles through many more instances than it ever runs at once is thrashing under the
        provision/teardown loop (workers disconnect mid-flight, tasks retry). Steady-load only: do NOT copy
        onto tests that scale up AND down, which legitimately create more instances than the peak."""
        square, _ = _make_tasks()
        stop = threading.Event()
        peak = 0
        created: set = set()

        def sample() -> None:
            nonlocal peak
            while not stop.is_set():
                names = running_task_container_names(FLOCI_INSTANCE_CONTAINER_PREFIX)
                peak = max(peak, len(names))
                created.update(names)
                time.sleep(_CHURN_SAMPLE_SECONDS)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            with Client(self.harness.scheduler_address, timeout_seconds=_CLIENT_TIMEOUT_SECONDS) as client:
                deadline = time.monotonic() + _CHURN_LOAD_SECONDS
                while time.monotonic() < deadline:
                    client.map(square, range(_BURST_TASKS))  # steady, back-to-back waves of load
        finally:
            stop.set()
            sampler.join(timeout=5.0)

        total_created = len(created)
        print(f"ec2 scaling churn: created {total_created} instance(s), peak {peak} concurrent")
        self.assertGreaterEqual(total_created, 1, "no EC2 instance was provisioned under the steady load")
        self.assertLessEqual(
            total_created,
            peak + _CHURN_TOLERANCE,
            f"pool thrashed: created {total_created} instances but only {peak} ran concurrently at the peak "
            f"(steady load should settle on a stable pool, not repeatedly provision and tear down)",
        )


if __name__ == "__main__":
    unittest.main()
