import os
from typing import Dict

from scaler.config.common.cloud_children import CloudChildrenConfig
from scaler.config.constants import CHILD_DRAIN_TIMEOUT_MARGIN_SECONDS
from scaler.config.types.address import AddressConfig
from scaler.utility.network_util import resolve_port_zero


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


def child_link_arguments(
    children_address: AddressConfig,
    cloud_children_config: CloudChildrenConfig,
    unit_id: str,
    task_concurrency: int,
    drain_timeout_seconds: int,
) -> str:
    """The command line arguments that make a child native manager serve this manager as unit `unit_id`.

    `children_address` is the address this manager bound; an advertised address naming port 0 takes its port.
    """
    advertised_address = cloud_children_config.advertised_children_address
    parent_address = (
        children_address if advertised_address is None else resolve_port_zero(advertised_address, children_address)
    )
    child_drain_timeout_seconds = max(0, drain_timeout_seconds - CHILD_DRAIN_TIMEOUT_MARGIN_SECONDS)
    return (
        f"--parent-address {parent_address} "
        f"--unit-id {unit_id} "
        f"--max-task-concurrency {task_concurrency} "
        f"--drain-timeout-seconds {child_drain_timeout_seconds}"
    )
