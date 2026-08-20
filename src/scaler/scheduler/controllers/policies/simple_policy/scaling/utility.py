from typing import Optional, Tuple

from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.static import StaticScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy

_ARGUMENT_SEPARATOR = ":"


def parse_scaling_policy_token(token: str) -> Tuple[ScalingPolicyStrategy, Optional[int]]:
    """Split a scaling token into its strategy and its optional argument.

    The token is `name` or `name:argument`, for example `vanilla` or `static:8`. Only the static
    strategy reads an argument.
    """
    name, separator, argument = token.partition(_ARGUMENT_SEPARATOR)

    strategy = ScalingPolicyStrategy(name.strip())

    if not separator:
        return strategy, None

    if strategy != ScalingPolicyStrategy.STATIC:
        raise ValueError(f"scaling strategy {name.strip()!r} takes no argument, got {argument.strip()!r}")

    try:
        return strategy, int(argument.strip())
    except ValueError:
        raise ValueError(f"static scaling argument must be an integer, got {argument.strip()!r}") from None


def create_scaling_policy(
    scaling_policy_strategy: ScalingPolicyStrategy, argument: Optional[int] = None
) -> ScalingPolicy:
    if scaling_policy_strategy == ScalingPolicyStrategy.VANILLA:
        return VanillaScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.CAPABILITY:
        return CapabilityScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.STATIC:
        return StaticScalingPolicy(argument)

    raise ValueError(f"unsupported scaling policy strategy: {scaling_policy_strategy}")
