"""The simulated 'cloud' worker manager for the container-scaling e2e.

``ContainerWorkerProvisioner`` is a real ``DeclarativeWorkerProvisioner`` (driven by the scheduler
through ``WorkerManagerRunner``, exactly like the cloud managers), but each provisioned "machine" is a
container running a *fixed* ``baremetal_native`` worker manager -- the local, free analog of a cloud
manager provisioning an instance whose user-data launches a worker. Configurable boot/shutdown delays
simulate cloud latency, and each container gets its own IP so tests can exercise multi-machine spread
and nested clients with distinct network identities.

Machines run the self-contained worker image (``_container_image``), so they are byte-identical to the
host scheduler with no host-layout coupling; the repo is bind-mounted read-only only so a container can
import the test's task module (``tests.integration._tasks``), which the wheel does not ship.

``worker_manager_id`` / ``name_prefix`` are parameterized so several provisioners (e.g. two priorities
in a waterfall policy) can coexist on one scheduler.
"""

from __future__ import annotations

import asyncio
import math
import os
from typing import List

from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from tests.integration._container_image import DEFAULT_IMAGE_TAG
from tests.integration._container_runtime import ContainerRuntime, DockerRuntime

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_WORKER_ENTRYPOINT = "scaler_worker_manager"  # on PATH inside the worker image
_DEFAULT_NAME_PREFIX = "scaler-it-machine"


def repo_bind_mounts() -> List[str]:
    """Read-only repo mount so a container worker can import ``tests.integration._tasks`` (not in the
    wheel). The container path equals the host path, so it doubles as the worker's ``PYTHONPATH``."""
    return [f"{_REPO_ROOT}:{_REPO_ROOT}:ro"]


class ContainerWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(
        self,
        runtime: ContainerRuntime,
        worker_scheduler_address: str,
        workers_per_machine: int,
        max_machines: int,
        boot_delay_seconds: float = 0.0,
        shutdown_delay_seconds: float = 0.0,
        image: str = DEFAULT_IMAGE_TAG,
        name_prefix: str = _DEFAULT_NAME_PREFIX,
    ) -> None:
        self._runtime = runtime
        self._worker_scheduler_address = worker_scheduler_address  # gateway-reachable, for the containers
        self._workers = workers_per_machine
        self._boot_delay = boot_delay_seconds
        self._shutdown_delay = shutdown_delay_seconds
        self._image = image
        self._name_prefix = name_prefix
        self._volumes = repo_bind_mounts()
        self._units: List[str] = []
        self._counter = 0
        self._coordinator = CapacityCoordinator(
            start_units=self.start_units,
            stop_units=self.stop_units,
            active_unit_count=self.active_unit_count,
            max_unit_count=max_machines,
        )

    def active_unit_count(self) -> int:
        return len(self._units)

    async def set_desired_task_concurrency(self, requests) -> None:
        task_concurrency = extract_desired_count(requests, {})
        desired = math.ceil(task_concurrency / self._workers) if self._workers > 0 else task_concurrency
        await self._coordinator.set_desired_unit_count(desired)

    async def start_units(self, count: int) -> None:
        if self._boot_delay:
            await asyncio.sleep(self._boot_delay)  # simulate cloud instance boot latency
        for _ in range(count):
            self._counter += 1
            name = f"{self._name_prefix}-{self._counter}"
            command = [
                _WORKER_ENTRYPOINT,
                "baremetal_native",
                self._worker_scheduler_address,
                "--worker-manager-id",
                name,
                "--mode",
                "fixed",
                "--num-of-workers",
                str(self._workers),
                # Prefetch only one task per worker so the scheduler's queue stays non-empty while machines
                # are in use; otherwise workers hoard the whole burst, the queue looks idle, and the scaling
                # policy tears the machine down mid-flight.
                "--per-worker-task-queue-size",
                "1",
            ]
            # PYTHONPATH=repo root so the worker can import the task module by reference (the wheel only ships
            # src/, not tests/); SCALER_IT_MACHINE_ID tags workers with their machine for spread/rebalance tests.
            env = {"PYTHONPATH": _REPO_ROOT, "SCALER_IT_MACHINE_ID": name}
            self._units.append(await self._runtime.run(self._image, name, command, env=env, volumes=self._volumes))

    async def stop_units(self, count: int) -> None:
        if self._shutdown_delay:
            await asyncio.sleep(self._shutdown_delay)  # simulate cloud instance shutdown latency
        for _ in range(min(count, len(self._units))):
            await self._runtime.stop(self._units.pop(0))

    async def terminate(self) -> None:
        self._coordinator.cancel()
        for container in self._units:
            await self._runtime.stop(container)
        self._units.clear()


def run_container_worker_manager(
    scheduler_address: str,
    worker_scheduler_address: str,
    workers_per_machine: int,
    max_machines: int = 8,
    boot_delay_seconds: float = 0.0,
    shutdown_delay_seconds: float = 0.0,
    worker_manager_id: str = "wm-container-it",
    name_prefix: str = _DEFAULT_NAME_PREFIX,
) -> None:
    """Process entry point for a container worker manager: it connects to the scheduler over loopback
    (``scheduler_address``) and provisions container machines that reach it via ``worker_scheduler_address``
    (the docker-bridge gateway). ``worker_manager_id`` is how this provisioner is addressed by the
    scheduler's scaling policy (e.g. a waterfall priority rule); ``name_prefix`` namespaces its containers."""
    from scaler.config.defaults import DEFAULT_HEARTBEAT_INTERVAL_SECONDS, DEFAULT_IO_THREADS
    from scaler.config.types.address import AddressConfig
    from scaler.utility.logging.utility import setup_logger
    from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

    setup_logger()
    provisioner = ContainerWorkerProvisioner(
        runtime=DockerRuntime(),
        worker_scheduler_address=worker_scheduler_address,
        workers_per_machine=workers_per_machine,
        max_machines=max_machines,
        boot_delay_seconds=boot_delay_seconds,
        shutdown_delay_seconds=shutdown_delay_seconds,
        name_prefix=name_prefix,
    )
    WorkerManagerRunner(
        address=AddressConfig.from_string(scheduler_address),
        name="worker_manager_container",
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        capabilities={},
        max_provisioner_units=max_machines,
        worker_manager_id=worker_manager_id.encode(),
        worker_provisioner=provisioner,
        io_threads=DEFAULT_IO_THREADS,
        workers_per_provisioner_unit=workers_per_machine,
    ).run()
