from typing import Optional

from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.static import StaticScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy


def create_scaling_policy(scaling: str) -> ScalingPolicy:
    """Build a scaling policy from its `name[:argument]` string. Only `static` takes an argument."""
    name, _, argument = scaling.partition(":")
    scaling_policy_strategy = ScalingPolicyStrategy(name)

    if scaling_policy_strategy == ScalingPolicyStrategy.STATIC:
        return StaticScalingPolicy(_parse_static_task_concurrency(argument))

    if argument:
        raise ValueError(f"scaling policy {name!r} takes no argument, got {scaling!r}")

    if scaling_policy_strategy == ScalingPolicyStrategy.VANILLA:
        return VanillaScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.CAPABILITY:
        return CapabilityScalingPolicy()

    raise ValueError(f"unsupported scaling policy strategy: {scaling_policy_strategy}")


def _parse_static_task_concurrency(argument: str) -> Optional[int]:
    if not argument:
        return None

    if not argument.isdigit():
        raise ValueError(f"static scaling takes a non-negative task concurrency, got {argument!r}")

    return int(argument)
