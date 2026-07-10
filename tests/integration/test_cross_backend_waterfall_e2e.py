"""Cross-backend waterfall end-to-end: an ECS and an EC2 worker manager on ONE scheduler.

Where each single-backend e2e drives one shipped worker manager, this puts two DIFFERENT shipped managers
-- ``ECSWorkerManager`` (priority 1) and ``ORBAWSEC2WorkerManager`` (priority 2) -- behind one scheduler
running the ``waterfall_v1`` policy, and proves work fills the fast ECS pool first and only spills onto the
slower EC2 pool once ECS is saturated. Both managers point boto3 at the same floci emulator, which launches
real ECS ``RunTask`` containers and real EC2 ``RunInstances`` Amazon Linux 2023 instances as host siblings.

The shipped provisioners set no machine id, so a task cannot tag which manager ran it by hostname; instead
work is attributed by the floci container prefix -- ``floci-ecs-*`` for the ECS pool, ``floci-ec2-*`` for
the EC2 pool -- which is exactly what ``running_task_containers`` counts. The ECS manager is capped at one
task's worth of concurrency by its waterfall rule, so a burst deeper than that must overflow to EC2.

Opt-in (``RUN_CROSS_BACKEND_E2E=1``) and Docker-only, and -- like the EC2 e2e -- it needs a prebuilt
manylinux wheel of the current source (``scripts/build_cibuildwheel.sh``); it skips itself if that wheel is
absent. EC2 boots are real installs, so the spill timeout is generous. The harness also starts the web GUI
wired to the scheduler monitor.
"""

from __future__ import annotations

import os
import sys
import time
import unittest
from multiprocessing import get_context
from multiprocessing.process import BaseProcess
from typing import Optional

from scaler import Client
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import get_available_tcp_port
from tests.integration import CROSS_BACKEND_E2E_SKIP_REASON, RUN_CROSS_BACKEND_E2E
from tests.integration._container_image import DEFAULT_ECS_IMAGE_TAG, ensure_ec2_base_image, ensure_ecs_worker_image
from tests.integration._container_runtime import DockerRuntime
from tests.integration._ec2_backend import (
    WHEEL_DIR,
    manylinux_wheel,
    run_ec2_worker_manager,
    serve_directory_on_gateway,
)
from tests.integration._ecs_backend import run_ecs_worker_manager
from tests.integration._floci import (
    FLOCI_INSTANCE_CONTAINER_PREFIX,
    FLOCI_TASK_CONTAINER_PREFIX,
    FlociEmulator,
    floci_available,
    remove_task_containers,
    running_task_containers,
)
from tests.integration._harness import SchedulerHarness, assert_backend_processes_alive
from tests.utility.utility import logging_test_name, terminate_process

_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"

# Waterfall: ECS (priority 1) is capped at one task's worth of concurrency; overflow spills to EC2
# (priority 2). ecs_task_cpu is the workers per ECS task, so a cap equal to it holds ECS at a single task.
_ECS_MANAGER = "wm-ecs-waterfall"
_EC2_MANAGER = "wm-ec2-waterfall"
_ECS_TASK_CPU = 2
_ECS_CAP = _ECS_TASK_CPU
# EC2 (priority 2) may bring up a couple of instances to absorb the overflow beyond ECS's cap.
_EC2_MAX_CONCURRENCY = 4

_TASK_SECONDS = 0.15
# A burst far deeper than ECS's cap, so sustained overflow has to spill onto EC2.
_WATERFALL_TASKS = 60
# An EC2 "boot" is a real install (minutes), so give the overflow ample time to bring an instance up.
_SPILL_TIMEOUT_SECONDS = 420.0
# Minute-plus EC2 boots stress the scheduler while the pool scales, stalling the client heartbeat past the
# stock 60s; size the client/worker liveness to the provisioning latency so a healthy run is not cut off.
_CLIENT_TIMEOUT_SECONDS = 300
_WORKER_TIMEOUT_SECONDS = 120


def _make_square():
    """Return ``square`` as a nested function so cloudpickle serializes it BY VALUE: neither task image
    mounts the test module, so a module-level task would fail to import there."""

    def square(value: int) -> int:
        import time

        time.sleep(_TASK_SECONDS)
        return value * value

    return square


_WHEEL = manylinux_wheel()
_RUN_CROSS_BACKEND_E2E = RUN_CROSS_BACKEND_E2E and floci_available() and bool(_WHEEL)
if not RUN_CROSS_BACKEND_E2E:
    _SKIP_REASON = CROSS_BACKEND_E2E_SKIP_REASON
elif not floci_available():
    _SKIP_REASON = "Docker is required for the floci-backed cross-backend waterfall e2e"
else:
    _SKIP_REASON = f"no manylinux wheel under {WHEEL_DIR}; build one with scripts/build_cibuildwheel.sh"


@unittest.skipUnless(_RUN_CROSS_BACKEND_E2E, _SKIP_REASON)
class TestCrossBackendWaterfallE2E(unittest.TestCase):
    """A shipped ECS manager (priority 1) and a shipped EC2 manager (priority 2) on one scheduler."""

    @classmethod
    def setUpClass(cls) -> None:
        ensure_ecs_worker_image()
        ensure_ec2_base_image()

    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)
        self.runtime = DockerRuntime()
        gateway = self.runtime.host_gateway()
        self.addCleanup(remove_task_containers, FLOCI_TASK_CONTAINER_PREFIX)
        self.addCleanup(remove_task_containers, FLOCI_INSTANCE_CONTAINER_PREFIX)

        self.floci = FlociEmulator().start()
        self.addCleanup(self.floci.stop)

        wheel_port = get_available_tcp_port()
        self._wheel_server = serve_directory_on_gateway(WHEEL_DIR, "0.0.0.0", wheel_port)
        self.addCleanup(self._wheel_server.shutdown)
        self._wheel_url = f"http://{gateway}:{wheel_port}/{os.path.basename(_WHEEL)}"

        # priority,worker_manager_id[,max_task_concurrency]: ECS first (capped), EC2 as the overflow tier.
        rules = f"1,{_ECS_MANAGER},{_ECS_CAP}\n2,{_EC2_MANAGER}"
        self.harness = SchedulerHarness(
            policy_content=rules,
            policy_engine_type="waterfall_v1",
            gateway=gateway,
            enable_webgui=True,
            client_timeout_seconds=_CLIENT_TIMEOUT_SECONDS,
            worker_timeout_seconds=_WORKER_TIMEOUT_SECONDS,
        )
        self.addCleanup(self.harness.shutdown)
        self._ecs_process: Optional[BaseProcess] = None
        self._ec2_process: Optional[BaseProcess] = None

    def tearDown(self) -> None:
        # Runs before the addCleanup teardown, so the scheduler/managers are still in their post-test state:
        # if any crashed under churn, fail with that instead of the client's downstream TimeoutError.
        assert_backend_processes_alive(self, self.harness, ecs_manager=self._ecs_process, ec2_manager=self._ec2_process)

    def _start_ecs_manager(self) -> None:
        self._ecs_process = get_context("spawn").Process(
            target=run_ecs_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                self.floci.endpoint_url,
                DEFAULT_ECS_IMAGE_TAG,
                _ECS_TASK_CPU,
                _ECS_CAP,
                _ECS_MANAGER,
            ),
        )
        self._ecs_process.start()
        self.addCleanup(terminate_process, self._ecs_process)

    def _start_ec2_manager(self) -> None:
        self._ec2_process = get_context("spawn").Process(
            target=run_ec2_worker_manager,
            args=(
                self.harness.scheduler_address,
                self.harness.worker_scheduler_address,
                self.floci.endpoint_url,
                self._wheel_url,
                _PYTHON_VERSION,
                _EC2_MAX_CONCURRENCY,
                _EC2_MANAGER,
            ),
        )
        self._ec2_process.start()
        self.addCleanup(terminate_process, self._ec2_process)

    def test_waterfall_fills_ecs_then_spills_to_ec2(self) -> None:
        """With ECS at priority 1 (capped at one task) and EC2 at priority 2, a warmup burst runs entirely
        on ECS while no EC2 manager exists; once the EC2 manager joins, sustained overflow beyond ECS's cap
        spills onto EC2 -- real instances come up (floci-ec2 containers appear) and correct results
        throughout prove both backends ran the work under one scheduler."""
        square = _make_square()
        with Client(self.harness.scheduler_address, timeout_seconds=_CLIENT_TIMEOUT_SECONDS) as client:
            # Phase 1: ECS (priority 1) alone absorbs the warmup burst; there is no EC2 manager yet.
            self._start_ecs_manager()
            warmup = client.map(square, range(_WATERFALL_TASKS))
            self.assertEqual(warmup, [value * value for value in range(_WATERFALL_TASKS)])
            self.assertEqual(
                running_task_containers(FLOCI_INSTANCE_CONTAINER_PREFIX),
                0,
                "an EC2 instance came up before the EC2 manager was started",
            )

            # Phase 2: bring up the EC2 manager (priority 2); sustained overflow beyond ECS's cap must spill.
            self._start_ec2_manager()
            saw_ec2 = False
            deadline = time.monotonic() + _SPILL_TIMEOUT_SECONDS
            while not saw_ec2 and time.monotonic() < deadline:
                batch = client.map(square, range(_WATERFALL_TASKS))
                self.assertEqual(batch, [value * value for value in range(_WATERFALL_TASKS)])
                saw_ec2 = running_task_containers(FLOCI_INSTANCE_CONTAINER_PREFIX) > 0

        print(f"cross-backend waterfall: sustained overflow spilled ECS -> EC2 = {saw_ec2}")
        self.assertTrue(saw_ec2, "sustained overflow beyond ECS's cap never spilled onto the EC2 manager")


if __name__ == "__main__":
    unittest.main()
