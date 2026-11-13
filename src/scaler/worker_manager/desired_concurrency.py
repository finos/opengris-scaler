from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

if TYPE_CHECKING:
    from scaler.protocol.capnp import WorkerManagerCommand


def extract_desired_count(
    requests: List[WorkerManagerCommand.DesiredTaskConcurrencyRequest], own_capabilities: Dict[str, int]
) -> int:
    """Return the desired worker count for this provisioner from a declarative scaling command.

    Sums taskConcurrency across all requests whose capability set is satisfiable by own_capabilities.
    A request capability matches when the provisioner has that capability with a value >= the request, or when the
    provisioner's value is -1 (unlimited). An empty capability set in a request acts as a wildcard that matches any
    provisioner. Returns 0 if no request matches.
    """
    total = 0
    for request in requests:
        request_capabilities = {entry.name: entry.value for entry in request.capabilities}
        if _capabilities_satisfy(request_capabilities, own_capabilities):
            total += request.taskConcurrency
    return total


def _capabilities_satisfy(request_capabilities: Dict[str, int], own_capabilities: Dict[str, int]) -> bool:
    """Return True if own_capabilities can satisfy every capability in request_capabilities.

    A capability is satisfied when the provisioner has it with a value >= the request,
    or when the provisioner's value is -1 (unlimited).
    """
    for name, request_value in request_capabilities.items():
        if name not in own_capabilities:
            return False
        own_value = own_capabilities[name]
        if own_value != -1 and own_value < request_value:
            return False
    return True
