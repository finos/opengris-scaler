"""Concrete worker-manager backends for the framework. Each one only supplies the seams the scenarios can't
know: a boot-latency-matched ``Profile``, how to build its task image, and how to provision one manager and
observe its pool. Everything downstream (harness, floci, scenarios, tasks) is shared.
"""

from __future__ import annotations

import math
import sys
from multiprocessing import get_context
from typing import Callable, Optional

from scaler.config.defaults import DEFAULT_CLIENT_TIMEOUT_SECONDS, DEFAULT_WORKER_TIMEOUT_SECONDS
from tests.integration._container_backend import run_container_worker_manager
from tests.integration._container_image import (
    DEFAULT_ECS_IMAGE_TAG,
    ensure_ec2_base_image,
    ensure_ecs_worker_image,
    ensure_worker_image,
)
from tests.integration._ec2_backend import run_ec2_worker_manager
from tests.integration._ecs_backend import run_ecs_worker_manager
from tests.integration._floci import FLOCI_INSTANCE_CONTAINER_PREFIX, FLOCI_TASK_CONTAINER_PREFIX
from tests.integration.e2e.framework import ContainerPool, DeployContext, ManagerHandle, Profile

_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"


def _max_machines_for_cap(cap: Optional[int], default: int, workers_per_machine: int) -> int:
    """A waterfall rule caps concurrency; translate that to the machine count a container/EC2 manager may run."""
    if cap is None:
        return default
    return max(1, math.ceil(cap / workers_per_machine))


def _spawn_manager(
    ctx: DeployContext, worker_manager_id: str, priority: int, prefix: str, target: Callable, args: tuple
) -> ManagerHandle:
    """Spawn one manager entry point in its own process and pair it with a prefix-scoped pool observer.
    Shared by every backend -- only ``target``, ``args``, and the container-name ``prefix`` differ."""
    process = get_context("spawn").Process(target=target, args=args)
    process.start()
    return ManagerHandle(worker_manager_id, priority, ContainerPool(prefix, ctx.cli), process)


class FlociEcsBackend:
    """The shipped ECS worker manager against floci: launches real ECS ``RunTask`` sibling containers that
    boot in seconds from a prebaked image (so no wheel and the stock liveness timeouts suffice)."""

    name = "ecs"
    needs_floci = True
    needs_wheel = False

    def __init__(self, task_cpu: int = 2, max_task_concurrency: int = 6) -> None:
        # ecs_task_cpu = workers per task and the scale-up divisor; the cap allows up to ceil(mtc / cpu) tasks.
        self._task_cpu = task_cpu
        self._max_task_concurrency = max_task_concurrency

    def profile(self) -> Profile:
        return Profile(
            task_seconds=0.15,
            burst_tasks=24,
            held_load_tasks=30,
            warmup_tasks=4,
            provision_timeout=60.0,
            drain_timeout=90.0,
            spread_timeout=150.0,
            churn_window=45.0,
            churn_sample=0.4,
            churn_tolerance=2,
            client_timeout=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout=DEFAULT_WORKER_TIMEOUT_SECONDS,
        )

    def ensure_image(self) -> None:
        ensure_ecs_worker_image()

    def provision(self, ctx: DeployContext, worker_manager_id: str, priority: int, cap: Optional[int]) -> ManagerHandle:
        args = (
            ctx.harness.scheduler_address,
            ctx.harness.worker_scheduler_address,
            ctx.floci_endpoint,
            DEFAULT_ECS_IMAGE_TAG,
            self._task_cpu,
            cap if cap is not None else self._max_task_concurrency,
            worker_manager_id,
        )
        return _spawn_manager(
            ctx, worker_manager_id, priority, FLOCI_TASK_CONTAINER_PREFIX, run_ecs_worker_manager, args
        )


class FlociEc2Backend:
    """The shipped ORB/EC2 worker manager against floci: launches real Amazon Linux 2023 instances that
    install a current-source manylinux wheel and boot -- minute-long boots, so it uses a generous profile
    and cloud-realistic liveness timeouts."""

    name = "ec2"
    needs_floci = True
    needs_wheel = True

    def __init__(self, max_task_concurrency: int = 4) -> None:
        self._max_task_concurrency = max_task_concurrency

    def profile(self) -> Profile:
        return Profile(
            task_seconds=0.15,
            burst_tasks=24,
            held_load_tasks=24,
            warmup_tasks=4,
            provision_timeout=300.0,
            drain_timeout=180.0,
            spread_timeout=420.0,
            churn_window=180.0,
            churn_sample=1.0,
            churn_tolerance=3,
            client_timeout=300,
            worker_timeout=120,
            poll=1.0,
        )

    def ensure_image(self) -> None:
        ensure_ec2_base_image()

    def provision(self, ctx: DeployContext, worker_manager_id: str, priority: int, cap: Optional[int]) -> ManagerHandle:
        args = (
            ctx.harness.scheduler_address,
            ctx.harness.worker_scheduler_address,
            ctx.floci_endpoint,
            ctx.wheel_url,
            _PYTHON_VERSION,
            cap if cap is not None else self._max_task_concurrency,
            worker_manager_id,
        )
        return _spawn_manager(
            ctx, worker_manager_id, priority, FLOCI_INSTANCE_CONTAINER_PREFIX, run_ec2_worker_manager, args
        )


class ContainerBackend:
    """A container "machine" manager with no cloud at all: each machine is a fixed baremetal_native worker in
    its own container. Boots in seconds but churns decisively under the vanilla policy (a test-double
    provisioner, unlike the shipped cloud managers). A per-instance prefix lets two run at different waterfall
    tiers with distinguishable pools."""

    name = "container"
    needs_floci = False
    needs_wheel = False

    def __init__(self, prefix: str = "scaler-it-machine", workers_per_machine: int = 2, max_machines: int = 3) -> None:
        self._prefix = prefix
        self._workers_per_machine = workers_per_machine
        self._max_machines = max_machines

    def profile(self) -> Profile:
        return Profile(
            task_seconds=0.15,
            burst_tasks=24,
            held_load_tasks=30,
            warmup_tasks=4,
            provision_timeout=60.0,
            drain_timeout=90.0,
            spread_timeout=120.0,
            churn_window=45.0,
            churn_sample=0.4,
            churn_tolerance=2,
            client_timeout=DEFAULT_CLIENT_TIMEOUT_SECONDS,
            worker_timeout=DEFAULT_WORKER_TIMEOUT_SECONDS,
        )

    def ensure_image(self) -> None:
        ensure_worker_image()

    def provision(self, ctx: DeployContext, worker_manager_id: str, priority: int, cap: Optional[int]) -> ManagerHandle:
        max_machines = _max_machines_for_cap(cap, self._max_machines, self._workers_per_machine)
        args = (
            ctx.harness.scheduler_address,
            ctx.harness.worker_scheduler_address,
            self._workers_per_machine,
            max_machines,
            0.0,
            0.0,
            worker_manager_id,
            self._prefix,
        )
        return _spawn_manager(ctx, worker_manager_id, priority, self._prefix, run_container_worker_manager, args)
