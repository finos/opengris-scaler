"""The simulated 'cloud' worker manager for the container-scaling e2e skeleton.

``ContainerWorkerProvisioner`` is a real ``DeclarativeWorkerProvisioner`` (driven by the scheduler
through ``WorkerManagerRunner``, exactly like the cloud managers), but each provisioned "machine" is a
Docker container running a *fixed* ``baremetal_native`` worker manager -- the local, free analog of a
cloud manager provisioning an instance whose user-data launches a worker. Configurable boot/shutdown
delays simulate cloud latency, and each container gets its own IP so tests can exercise multi-machine
spread and nested clients with distinct network identities.

The container runs the host's already-built scaler via read-only bind-mounts (no image build, exact
version/protocol match with the host scheduler); see ``scaler_bind_mounts``.
"""

from __future__ import annotations

import asyncio
import math
import os
import sys
from typing import List

from scaler.worker_manager_adapter.capacity_coordinator import CapacityCoordinator
from scaler.worker_manager_adapter.common import extract_desired_count
from scaler.worker_manager_adapter.mixins import DeclarativeWorkerProvisioner
from tests.integration._container_runtime import ContainerRuntime, DockerRuntime

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_WORKER_IMAGE = os.environ.get("SCALER_IT_WORKER_IMAGE", "ubuntu:26.04")


def scaler_bind_mounts() -> List[str]:
    """Read-only bind-mounts that make the host's built scaler runnable in the container: the repo (its
    editable install + the venv), the uv interpreter store, and the C++ runtime libs.

    The venv's ``python`` symlinks to a version-agnostic name (``.../uv/python/cpython-3.13-.../``) that
    in turn points at the versioned dir (``cpython-3.13.14-...``); both live in the uv store, so mount
    the whole ``.../uv`` root -- mounting only the versioned dir leaves the shebang symlink dangling."""
    real_python = os.path.realpath(sys.executable)  # .../uv/python/cpython-X.Y.Z-.../bin/python3.13
    uv_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(real_python))))  # .../uv
    return [f"{_REPO_ROOT}:{_REPO_ROOT}:ro", f"{uv_root}:{uv_root}:ro", "/usr/local/lib:/usr/local/lib:ro"]


def worker_manager_entrypoint() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(sys.executable)), "scaler_worker_manager")


class ContainerWorkerProvisioner(DeclarativeWorkerProvisioner):
    def __init__(
        self,
        runtime: ContainerRuntime,
        worker_scheduler_address: str,
        workers_per_machine: int,
        max_machines: int,
        boot_delay_seconds: float = 0.0,
        shutdown_delay_seconds: float = 0.0,
        image: str = DEFAULT_WORKER_IMAGE,
        name_prefix: str = "scaler-it-machine",
    ) -> None:
        self._runtime = runtime
        self._worker_scheduler_address = worker_scheduler_address  # gateway-reachable, for the containers
        self._workers = workers_per_machine
        self._boot_delay = boot_delay_seconds
        self._shutdown_delay = shutdown_delay_seconds
        self._image = image
        self._name_prefix = name_prefix
        self._volumes = scaler_bind_mounts()
        self._entry = worker_manager_entrypoint()
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
                self._entry,
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
            # PYTHONPATH=repo root so the worker can import the task's module by reference (the editable
            # install only exposes src/, not tests/); SCALER_IT_MACHINE_ID tags workers with their machine.
            env = {"LD_LIBRARY_PATH": "/usr/local/lib", "PYTHONPATH": _REPO_ROOT, "SCALER_IT_MACHINE_ID": name}
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
) -> None:
    """Process entry point for the Level-1 container manager: it connects to the scheduler over loopback
    (``scheduler_address``) and provisions container machines that reach it via ``worker_scheduler_address``
    (the docker-bridge gateway)."""
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
    )
    WorkerManagerRunner(
        address=AddressConfig.from_string(scheduler_address),
        name="worker_manager_container",
        heartbeat_interval_seconds=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        capabilities={},
        max_provisioner_units=max_machines,
        worker_manager_id=b"wm-container-it",
        worker_provisioner=provisioner,
        io_threads=DEFAULT_IO_THREADS,
        workers_per_provisioner_unit=workers_per_machine,
    ).run()
