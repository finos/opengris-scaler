"""A tiny container-runtime abstraction for the container-scaling e2e skeleton.

Each simulated "machine" is a real container running a fixed ``baremetal_native`` worker manager -- the
local, free analog of a cloud manager provisioning an instance whose user-data launches
a worker. The manager code depends only on the ``ContainerRuntime`` interface, so podman (or another
runtime) can be dropped in behind the same interface without touching the provisioner.

Docker is invoked through ``sudo`` where the socket is root-only (see .agents-local.md); override the
whole invocation with ``SCALER_IT_CONTAINER_CLI`` (e.g. ``podman`` once a PodmanRuntime lands, or a
rootless ``docker``).
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Sequence

# Max seconds docker waits for a container to exit on stop before SIGKILL. Headroom for the worker's
# graceful task-finishing shutdown, not a fixed delay (docker returns as soon as the container exits).
_STOP_GRACE_SECONDS = 15


class ContainerRuntime(ABC):
    """Minimal surface the container worker manager needs: run detached, stop, and introspect."""

    @abstractmethod
    async def run(
        self,
        image: str,
        name: str,
        command: Sequence[str],
        env: Optional[Dict[str, str]] = None,
        volumes: Optional[Sequence[str]] = None,
    ) -> str:
        """Start a detached container on a bridge network (so it gets its own IP) and return its id.

        Async because it is driven from the worker manager's event loop: a blocking subprocess there
        both stalls heartbeats and can deadlock waitpid against the loop's child reaping.

        ``volumes`` are ``host:container[:mode]`` bind-mounts -- the test harness mounts the host's
        already-built scaler in read-only, so the container runs the exact same version as the scheduler
        without an image build."""

    @abstractmethod
    async def stop(self, container: str) -> None:
        """Stop and remove a container; must not raise if it is already gone."""

    @abstractmethod
    def is_running(self, container: str) -> bool: ...

    @abstractmethod
    def ip_address(self, container: str) -> str:
        """The container's address on its bridge network -- distinct per machine (nested-client tests)."""

    @abstractmethod
    def logs(self, container: str) -> str: ...

    @abstractmethod
    def host_gateway(self) -> str:
        """The address a bridge container uses to reach services on the host (the scheduler)."""


class DockerRuntime(ContainerRuntime):
    def __init__(self) -> None:
        self._cli: List[str] = os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()

    def _run_cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        # Sync helper for the introspection calls (invoked from sync test code, never the event loop).
        return subprocess.run(
            [*self._cli, *args], check=check, capture_output=True, text=True, stdin=subprocess.DEVNULL
        )

    async def _run_cli_async(self, *args: str, check: bool = True) -> str:
        # stdin=DEVNULL: a detached launch must never read stdin (no sudo/tty stall). create_subprocess_exec
        # keeps the event loop responsive and lets asyncio, not a blocking waitpid, reap the child.
        proc = await asyncio.create_subprocess_exec(
            *self._cli, *args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        if check and proc.returncode != 0:
            raise RuntimeError(f"`{self._cli[-1]} {args[0]}` failed ({proc.returncode}): {stderr.decode().strip()}")
        return stdout.decode()

    @staticmethod
    def is_available() -> bool:
        cli = os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()
        try:
            return subprocess.run([*cli, "info"], capture_output=True, timeout=30).returncode == 0
        except Exception:
            return False

    async def run(
        self,
        image: str,
        name: str,
        command: Sequence[str],
        env: Optional[Dict[str, str]] = None,
        volumes: Optional[Sequence[str]] = None,
    ) -> str:
        args: List[str] = []
        for key, value in (env or {}).items():
            args += ["-e", f"{key}={value}"]
        for volume in volumes or ():
            args += ["-v", volume]
        # --rm so an exited/killed machine leaves nothing behind; detached so start_units does not block.
        return (await self._run_cli_async("run", "-d", "--rm", "--name", name, *args, image, *command)).strip()

    async def stop(self, container: str) -> None:
        # docker stop sends SIGTERM; the native manager forwards SIGINT to its workers so each finishes
        # its in-flight task before exiting. Give that graceful drain real headroom -- docker returns as
        # soon as the container exits, so this only caps a genuinely stuck shutdown, it is not a fixed wait.
        await self._run_cli_async("stop", "-t", str(_STOP_GRACE_SECONDS), container, check=False)

    def is_running(self, container: str) -> bool:
        result = self._run_cli("inspect", "-f", "{{.State.Running}}", container, check=False)
        return result.returncode == 0 and result.stdout.strip() == "true"

    def ip_address(self, container: str) -> str:
        result = self._run_cli("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}", container)
        return result.stdout.strip()

    def logs(self, container: str) -> str:
        return self._run_cli("logs", container, check=False).stdout

    def host_gateway(self) -> str:
        result = self._run_cli("network", "inspect", "bridge", "-f", "{{(index .IPAM.Config 0).Gateway}}")
        return result.stdout.strip()
