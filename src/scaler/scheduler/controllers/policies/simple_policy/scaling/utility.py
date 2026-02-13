from scaler.scheduler.controllers.policies.simple_policy.scaling.capability_scaling import CapabilityScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.fixed_elastic import FixedElasticScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.no import NoScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingControllerStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.vanilla import VanillaScalingController


def create_scaling_controller(scaling_controller_strategy: ScalingControllerStrategy) -> ScalingController:
    if scaling_controller_strategy == ScalingControllerStrategy.NO:
        return NoScalingController()
    elif scaling_controller_strategy == ScalingControllerStrategy.VANILLA:
        return VanillaScalingController()
    elif scaling_controller_strategy == ScalingControllerStrategy.FIXED_ELASTIC:
        return FixedElasticScalingController()
    elif scaling_controller_strategy == ScalingControllerStrategy.CAPABILITY:
        return CapabilityScalingController()

    raise ValueError(f"unsupported scaling controller strategy: {scaling_controller_strategy}")
