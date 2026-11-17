import os
import re
from typing import Dict, Optional

from scaler.config.common.cloud_children import CloudChildrenConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.utility.telemetry import is_export_configured, otel_environment, trace_context_environment

# a requirement line that installs opengris-scaler with extras, e.g. "opengris-scaler[otel,aws]>=2.9"
SCALER_REQUIREMENT_EXTRAS = re.compile(r"^\s*opengris[-_.]scaler\s*\[([^\]]*)\]", re.IGNORECASE)
TELEMETRY_EXTRAS = frozenset({"otel", "all"})


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
    worker_manager_config: WorkerManagerConfig,
    cloud_children_config: CloudChildrenConfig,
    unit_id: str,
    task_concurrency: int,
) -> str:
    """The command line arguments that make a child native manager serve this manager as unit `unit_id`."""
    parent_address = cloud_children_config.advertised_children_address or worker_manager_config.children_address
    return f"--parent-address {parent_address} --unit-id {unit_id} --max-task-concurrency {task_concurrency}"


def unit_environment() -> Dict[str, str]:
    """The environment a unit starts with: this manager's OpenTelemetry settings, and the trace of the reconcile
    starting it, so the unit's manager and workers export the same way and join the scale-up trace."""
    return {**otel_environment(), **trace_context_environment()}


def check_unit_requirements_support_telemetry(requirements_content: Optional[str], telemetry_relay: bool) -> None:
    """Fail at start when units would export telemetry but their requirements leave out the OpenTelemetry SDK.

    A unit given an endpoint or the relay flag without the SDK exits at start, so every unit would fail."""
    if requirements_content is None or not (telemetry_relay or is_export_configured()):
        return

    for line in requirements_content.splitlines():
        match = SCALER_REQUIREMENT_EXTRAS.match(line)
        if match is None:
            continue
        extras = {extra.strip().lower() for extra in match.group(1).split(",")}
        if extras & TELEMETRY_EXTRAS:
            return

    raise ValueError(
        "units export telemetry (OTEL_EXPORTER_OTLP_ENDPOINT or --telemetry-relay is set), but their requirements "
        "install opengris-scaler without the otel extra: require opengris-scaler[otel]"
    )
