from scaler.scheduler.controllers.policies.library.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.fixed_elastic import FixedElasticScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.no import NoScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingPolicy


def create_scaling_policy(scaling_policy_strategy: ScalingPolicyStrategy) -> ScalingPolicy:
    if scaling_policy_strategy == ScalingPolicyStrategy.NO:
        return NoScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.VANILLA:
        return VanillaScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.FIXED_ELASTIC:
        return FixedElasticScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.CAPABILITY:
        return CapabilityScalingPolicy()

    raise ValueError(f"unsupported scaling policy strategy: {scaling_policy_strategy}")
