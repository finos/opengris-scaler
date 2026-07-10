"""Core types for the e2e scaling framework: the scenario-facing ``Profile`` and ``Deployment``, the
prefix-scoped ``ContainerPool`` observer, the abstract ``WorkerManagerBackend``, and the ``deploy`` builder
that composes the shared infrastructure (floci, wheel server, scheduler harness) with per-backend managers.

Every backend -- container "machines", floci ECS task containers, floci EC2 instance containers -- runs its
workers in host Docker containers, so a single prefix-scoped ``docker ps`` is a uniform view of any pool;
that is what lets one set of scenarios drive all of them.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess
from multiprocessing.process import BaseProcess
from typing import List, Optional, Protocol

from scaler.utility.network_util import get_available_tcp_port
from tests.integration._container_runtime import DockerRuntime
from tests.integration._floci import FlociEmulator
from tests.integration._harness import SchedulerHarness, assert_backend_processes_alive
from tests.integration.e2e.tasks import Tasks, make_tasks
from tests.utility.utility import terminate_process


def container_cli() -> List[str]:
    """The Docker/Podman CLI (``SCALER_IT_CONTAINER_CLI``), e.g. ``["sudo", "docker"]``."""
    return os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()


@dataclasses.dataclass(frozen=True)
class Profile:
    """Scenario timing and sizing, supplied by a backend and matched to its boot latency (a container boots
    in seconds, a real EC2 instance in minutes), so the same scenario logic stays honest on each."""

    task_seconds: float
    burst_tasks: int  # a deep burst (spread / rising-load / steady-load)
    held_load_tasks: int  # a load held in flight while sampling the scale-up peak (burst-and-drain)
    warmup_tasks: int  # a concurrency-1 trickle (rising-load)
    provision_timeout: float  # how long a held burst may take to bring the pool up
    drain_timeout: float  # how long an idle pool may take to reach zero
    spread_timeout: float  # how long to keep re-submitting until work has spread across units
    churn_window: float  # how long to hold a steady load while checking the pool settles
    churn_sample: float
    churn_tolerance: int
    client_timeout: int
    worker_timeout: int
    poll: float = 0.5


class ContainerPool:
    """One manager's pool of Docker containers, observed by name prefix. ``running_names`` accumulated over a
    run gives a churn-robust "created" count (each provisioned unit gets a fresh container name)."""

    def __init__(self, prefix: str, cli: Optional[List[str]] = None) -> None:
        self.prefix = prefix
        self._cli = cli or container_cli()

    def running_names(self) -> List[str]:
        result = subprocess.run(
            [*self._cli, "ps", "--filter", f"name={self.prefix}", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
        )
        return [name for name in result.stdout.split() if name]

    def running(self) -> int:
        return len(self.running_names())

    def reap(self) -> None:
        """Force-remove any container (running or stopped) with this prefix -- last-resort teardown for units
        a killed manager could not drain."""
        ids = subprocess.run(
            [*self._cli, "ps", "-aq", "--filter", f"name={self.prefix}"], capture_output=True, text=True
        ).stdout.split()
        for container in ids:
            subprocess.run([*self._cli, "rm", "-f", container], capture_output=True)


@dataclasses.dataclass
class ManagerHandle:
    """One provisioned worker manager: its scheduler id, waterfall priority, pool observer, and process."""

    worker_manager_id: str
    priority: int
    pool: ContainerPool
    process: BaseProcess


@dataclasses.dataclass
class DeployContext:
    """Shared infrastructure a backend provisions its manager against (built once by :func:`deploy`)."""

    harness: SchedulerHarness
    gateway: str
    cli: List[str]
    floci_endpoint: Optional[str]
    wheel_url: Optional[str]


class WorkerManagerBackend(Protocol):
    """How to provision ONE worker manager and observe its pool. Concrete backends live in e2e/backends.py;
    all shared logic (harness, floci, images) is composed in, never inherited."""

    name: str
    needs_floci: bool
    needs_wheel: bool

    def profile(self) -> Profile: ...

    def ensure_image(self) -> None: ...

    def provision(
        self, ctx: DeployContext, worker_manager_id: str, priority: int, cap: Optional[int]
    ) -> ManagerHandle: ...


@dataclasses.dataclass
class ManagerSpec:
    """A backend to run at one waterfall tier, optionally capped at a max task concurrency."""

    backend: WorkerManagerBackend
    cap: Optional[int] = None


class Deployment:
    """A scheduler harness bound to one or more worker managers. Scenarios read :attr:`profile`, submit via
    :attr:`tasks`, and observe the whole pool through :meth:`running` / :meth:`running_names`."""

    def __init__(self, harness: SchedulerHarness, handles: List[ManagerHandle], tasks: Tasks, profile: Profile):
        self.harness = harness
        self.handles = handles
        self.tasks = tasks
        self.profile = profile

    @property
    def pools(self) -> List[ContainerPool]:
        return [handle.pool for handle in self.handles]

    def running(self) -> int:
        return sum(pool.running() for pool in self.pools)

    def running_names(self) -> List[str]:
        names: List[str] = []
        for pool in self.pools:
            names.extend(pool.running_names())
        return names

    def assert_healthy(self, test_case) -> None:
        """Fail the test (naming the fault) if the scheduler or any manager crashed or wedged under churn."""
        processes = {handle.worker_manager_id: handle.process for handle in self.handles}
        assert_backend_processes_alive(test_case, self.harness, **processes)


def _merge_profiles(profiles: List[Profile]) -> Profile:
    """Combine the backends' profiles for a mixed deployment: take the most generous of each knob so the
    slowest backend's boot latency governs the timeouts and the deepest load governs the counts."""
    if len(profiles) == 1:
        return profiles[0]
    return Profile(
        task_seconds=max(profile.task_seconds for profile in profiles),
        burst_tasks=max(profile.burst_tasks for profile in profiles),
        held_load_tasks=max(profile.held_load_tasks for profile in profiles),
        warmup_tasks=max(profile.warmup_tasks for profile in profiles),
        provision_timeout=max(profile.provision_timeout for profile in profiles),
        drain_timeout=max(profile.drain_timeout for profile in profiles),
        spread_timeout=max(profile.spread_timeout for profile in profiles),
        churn_window=max(profile.churn_window for profile in profiles),
        churn_sample=min(profile.churn_sample for profile in profiles),
        churn_tolerance=max(profile.churn_tolerance for profile in profiles),
        client_timeout=max(profile.client_timeout for profile in profiles),
        worker_timeout=max(profile.worker_timeout for profile in profiles),
        poll=min(profile.poll for profile in profiles),
    )


def deploy(test_case, specs: List[ManagerSpec]) -> Deployment:
    """Bring up the shared infra and every manager, registering teardown on ``test_case``. One manager runs
    the vanilla policy; several run ``waterfall_v1`` with each spec at priority = its position (1 = highest),
    so ``deploy([ecs_spec, ec2_spec])`` is a cross-backend waterfall with no extra plumbing."""
    backends = [spec.backend for spec in specs]
    profile = _merge_profiles([backend.profile() for backend in backends])
    cli = container_cli()
    gateway = DockerRuntime().host_gateway()

    for backend in backends:
        backend.ensure_image()

    floci_endpoint = None
    if any(backend.needs_floci for backend in backends):
        floci = FlociEmulator().start()
        test_case.addCleanup(floci.stop)
        floci_endpoint = floci.endpoint_url

    wheel_url = None
    if any(backend.needs_wheel for backend in backends):
        from tests.integration._ec2_backend import WHEEL_DIR, manylinux_wheel, serve_directory_on_gateway

        wheel_port = get_available_tcp_port()
        server = serve_directory_on_gateway(WHEEL_DIR, "0.0.0.0", wheel_port)
        test_case.addCleanup(server.shutdown)
        wheel_url = f"http://{gateway}:{wheel_port}/{os.path.basename(manylinux_wheel())}"

    worker_manager_ids = [f"wm-{spec.backend.name}-p{index + 1}" for index, spec in enumerate(specs)]
    if len(specs) > 1:
        rules = "\n".join(
            f"{index + 1},{worker_manager_ids[index]}" + (f",{spec.cap}" if spec.cap is not None else "")
            for index, spec in enumerate(specs)
        )
        harness = SchedulerHarness(
            policy_content=rules,
            policy_engine_type="waterfall_v1",
            gateway=gateway,
            enable_webgui=True,
            client_timeout_seconds=profile.client_timeout,
            worker_timeout_seconds=profile.worker_timeout,
        )
    else:
        harness = SchedulerHarness(
            gateway=gateway,
            enable_webgui=True,
            client_timeout_seconds=profile.client_timeout,
            worker_timeout_seconds=profile.worker_timeout,
        )
    test_case.addCleanup(harness.shutdown)

    context = DeployContext(
        harness=harness, gateway=gateway, cli=cli, floci_endpoint=floci_endpoint, wheel_url=wheel_url
    )
    handles = []
    for index, spec in enumerate(specs):
        handle = spec.backend.provision(context, worker_manager_ids[index], index + 1, spec.cap)
        # LIFO teardown: terminate the manager (registered last) before reaping any container it orphaned.
        test_case.addCleanup(handle.pool.reap)
        test_case.addCleanup(terminate_process, handle.process)
        handles.append(handle)

    return Deployment(harness, handles, make_tasks(profile.task_seconds), profile)
