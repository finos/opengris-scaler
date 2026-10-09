import os
from typing import Dict

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.types.address import AddressConfig

# A child ends its drain this long before the parent's deadline, so the parent never destroys a resource whose
# fleet is still draining in good order.
CHILD_DRAIN_TIMEOUT_MARGIN_SECONDS = 30

# A resource boots and installs scaler before its child manager sends the first heartbeat.
CLOUD_UNIT_STARTUP_TIMEOUT_SECONDS = 600


def load_requirements_content(requirements_txt: str) -> str:
    """Return requirements file content, reading from disk if requirements_txt is a file path."""
    if os.path.isfile(requirements_txt):
        with open(requirements_txt) as f:
            return f.read()
    return requirements_txt


def format_capabilities(capabilities: Dict[str, int]) -> str:
    """
    Reverse of `parse_capabilities`: convert a capabilities dict into a
    comma-separated capability string (e.g. "linux,cpu=4").
    Values equal to -1 are emitted as flag-style entries (no `=value`).
    """
    parts = []
    for name, value in capabilities.items():
        if value == -1:
            parts.append(name)
        else:
            parts.append(f"{name}={value}")
    return ",".join(parts)


def cloud_children_address(worker_manager_config: WorkerManagerConfig) -> AddressConfig:
    """The address a cloud manager binds for the child managers in its resources to dial."""
    if worker_manager_config.children_address is None:
        raise ValueError(
            "a cloud worker manager needs children_address: an address its provisioned resources can reach"
        )
    return worker_manager_config.children_address


def child_link_arguments(worker_manager_config: WorkerManagerConfig, unit_id: str, task_concurrency: int) -> str:
    """The command line arguments that make a child native manager serve this manager as unit `unit_id`."""
    child_drain_timeout_seconds = max(
        0, worker_manager_config.drain_timeout_seconds - CHILD_DRAIN_TIMEOUT_MARGIN_SECONDS
    )
    return (
        f"--parent-address {cloud_children_address(worker_manager_config)} "
        f"--unit-id {unit_id} "
        f"--max-task-concurrency {task_concurrency} "
        f"--drain-timeout-seconds {child_drain_timeout_seconds}"
    )
