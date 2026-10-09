from typing import Dict, Optional

from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.static import StaticScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy, WorkerManagerBounds
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy


def create_scaling_policy(scaling: str) -> ScalingPolicy:
    """Build a scaling policy from its `name[:argument]` string, or `vanilla[bounds]`.

    Only `static` takes an argument, and only `vanilla` takes bounds."""
    name, bracket, bounds = scaling.partition("[")
    if bracket:
        if ScalingPolicyStrategy(name) != ScalingPolicyStrategy.VANILLA:
            raise ValueError(f"only the vanilla scaling policy takes bounds, got {scaling!r}")
        if not bounds.endswith("]"):
            raise ValueError(f"vanilla bounds must end with ']', got {scaling!r}")
        return VanillaScalingPolicy(_parse_vanilla_bounds(bounds[:-1]))

    name, _, argument = scaling.partition(":")
    scaling_policy_strategy = ScalingPolicyStrategy(name)

    if scaling_policy_strategy == ScalingPolicyStrategy.STATIC:
        return StaticScalingPolicy(_parse_static_task_concurrency(argument))

    if argument:
        raise ValueError(f"scaling policy {name!r} takes no argument, got {scaling!r}")

    if scaling_policy_strategy == ScalingPolicyStrategy.VANILLA:
        return VanillaScalingPolicy({})
    elif scaling_policy_strategy == ScalingPolicyStrategy.CAPABILITY:
        return CapabilityScalingPolicy()

    raise ValueError(f"unsupported scaling policy strategy: {scaling_policy_strategy}")


def _parse_static_task_concurrency(argument: str) -> Optional[int]:
    if not argument:
        return None

    if not argument.isdigit():
        raise ValueError(f"static scaling takes a non-negative task concurrency, got {argument!r}")

    return int(argument)


def _parse_vanilla_bounds(text: str) -> Dict[bytes, WorkerManagerBounds]:
    """Parse `worker_manager_id:max_task_concurrency[:min_task_concurrency]` entries separated by `,`.

    An empty max_task_concurrency leaves the cap to the manager's advertised maximum."""
    bounds: Dict[bytes, WorkerManagerBounds] = {}
    for entry in text.split(","):
        fields = [field.strip() for field in entry.split(":")]
        if len(fields) not in (2, 3) or not fields[0]:
            raise ValueError(
                f"vanilla bounds entry must be 'worker_manager_id:max_task_concurrency[:min_task_concurrency]', "
                f"got {entry.strip()!r}"
            )

        worker_manager_id = fields[0].encode()
        if worker_manager_id in bounds:
            raise ValueError(f"vanilla bounds name worker manager {fields[0]!r} twice")

        max_task_concurrency = _parse_count(fields[1], entry) if fields[1] else None
        min_task_concurrency = _parse_count(fields[2], entry) if len(fields) == 3 else 0
        if max_task_concurrency is not None and min_task_concurrency > max_task_concurrency:
            raise ValueError(f"vanilla bounds entry {entry.strip()!r}: the minimum exceeds the maximum")

        bounds[worker_manager_id] = WorkerManagerBounds(max_task_concurrency, min_task_concurrency)

    return bounds


def _parse_count(field: str, entry: str) -> int:
    if not field.isdigit():
        raise ValueError(f"vanilla bounds entry {entry.strip()!r}: {field!r} is not a non-negative task concurrency")

    return int(field)
