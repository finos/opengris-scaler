"""A managed floci AWS emulator for the ECS scaling e2e.

floci (``floci/floci:latest``) is a free, local AWS emulator that -- unlike moto or community LocalStack --
actually launches ECS ``RunTask`` containers as siblings on the host Docker daemon (through the mounted
docker socket). That makes it the one backend that drives the shipped ``ECSWorkerProvisioner`` through a
real scale curve without a paid tier or real AWS. It speaks the AWS API over plain HTTP on port 4566, so
the worker manager reaches it by pointing boto3 at ``AWS_ENDPOINT_URL``.

Spawned task containers land on floci's own Docker network (the default bridge), so their workers reach
the host scheduler over the bridge gateway exactly like the container e2e's machines. floci names each one
``floci-ecs-<taskid>-<container>`` and reaps it when the task exits.
"""

from __future__ import annotations

import os
import subprocess
import time
import urllib.error
import urllib.request
from typing import List

from scaler.utility.network_util import get_available_tcp_port

_FLOCI_IMAGE = os.environ.get("SCALER_IT_FLOCI_IMAGE", "floci/floci:latest")
_FLOCI_CONTAINER_NAME = "scaler-it-floci"
_FLOCI_INTERNAL_PORT = 4566
_DOCKER_SOCKET = "/var/run/docker.sock"
# floci names each launched container by the service that started it; used to count/reap them on teardown.
FLOCI_TASK_CONTAINER_PREFIX = "floci-ecs"  # ECS RunTask containers
FLOCI_INSTANCE_CONTAINER_PREFIX = "floci-ec2"  # EC2 RunInstances containers

_READY_TIMEOUT_SECONDS = 60.0
_READY_POLL_SECONDS = 0.5
_STOP_GRACE_SECONDS = 5


def _cli() -> List[str]:
    return os.environ.get("SCALER_IT_CONTAINER_CLI", "sudo docker").split()


def floci_available() -> bool:
    """True when the Docker daemon is reachable (floci needs it to launch sibling task containers)."""
    try:
        return subprocess.run([*_cli(), "info"], capture_output=True, timeout=30).returncode == 0
    except Exception:
        return False


def remove_task_containers(prefix: str = FLOCI_TASK_CONTAINER_PREFIX) -> None:
    """Reap any leftover floci-launched containers (last-resort cleanup if a run is killed before the
    manager can drain them). ``prefix`` selects ECS tasks (default) or EC2 instances."""
    cli = _cli()
    ids = subprocess.run(
        [*cli, "ps", "-aq", "--filter", f"name={prefix}"], capture_output=True, text=True
    ).stdout.split()
    for container in ids:
        subprocess.run([*cli, "rm", "-f", container], capture_output=True)


def running_task_container_names(prefix: str = FLOCI_TASK_CONTAINER_PREFIX) -> List[str]:
    """Names of the floci-launched containers currently running. floci gives each RunTask/RunInstances a
    fresh id (hence a fresh container name), so accumulating the DISTINCT set ever seen is a churn-robust
    CREATED count -- one a single running-set snapshot (which misses containers that already came and went)
    undercounts. ``prefix`` selects ECS tasks (default) or EC2 instances."""
    cli = _cli()
    result = subprocess.run(
        [*cli, "ps", "--filter", f"name={prefix}", "--format", "{{.Names}}"], capture_output=True, text=True
    )
    return [name for name in result.stdout.split() if name]


def running_task_containers(prefix: str = FLOCI_TASK_CONTAINER_PREFIX) -> int:
    """Number of floci-launched containers currently running -- the ground-truth pool size. ``prefix``
    selects ECS tasks (default) or EC2 instances."""
    return len(running_task_container_names(prefix))


class FlociEmulator:
    """Runs floci in a container with the host docker socket mounted, so ECS ``RunTask`` launches real
    sibling containers. Start it, read :attr:`endpoint_url`, and register :meth:`stop` with ``addCleanup``."""

    def __init__(self, image: str = _FLOCI_IMAGE) -> None:
        self._image = image
        self._cli = _cli()
        self._port = get_available_tcp_port()
        self._started = False

    @property
    def endpoint_url(self) -> str:
        return f"http://127.0.0.1:{self._port}"

    def start(self) -> "FlociEmulator":
        self._remove_existing()
        subprocess.run(
            [
                *self._cli,
                "run",
                "-d",
                "--rm",
                "--name",
                _FLOCI_CONTAINER_NAME,
                "-p",
                f"{self._port}:{_FLOCI_INTERNAL_PORT}",
                "-v",
                f"{_DOCKER_SOCKET}:{_DOCKER_SOCKET}",
                self._image,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        self._started = True
        self._wait_until_ready()
        return self

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + _READY_TIMEOUT_SECONDS
        last_error = "no response"
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(self.endpoint_url, timeout=2) as response:
                    response.read(1)
                return
            except urllib.error.HTTPError:
                return  # any HTTP status means the server is up and routing requests
            except Exception as error:
                last_error = repr(error)
                time.sleep(_READY_POLL_SECONDS)
        raise TimeoutError(
            f"floci did not become ready at {self.endpoint_url} within {_READY_TIMEOUT_SECONDS:.0f}s "
            f"(last error: {last_error}); logs:\n{self.logs()}"
        )

    def logs(self) -> str:
        return subprocess.run([*self._cli, "logs", _FLOCI_CONTAINER_NAME], capture_output=True, text=True).stdout

    def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        subprocess.run([*self._cli, "stop", "-t", str(_STOP_GRACE_SECONDS), _FLOCI_CONTAINER_NAME], capture_output=True)

    def _remove_existing(self) -> None:
        subprocess.run([*self._cli, "rm", "-f", _FLOCI_CONTAINER_NAME], capture_output=True)
