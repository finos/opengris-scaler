from typing import Tuple

from scaler.scheduler.controllers.policies.simple_policy.allocation.mixins import TaskAllocatePolicy
from scaler.scheduler.controllers.policies.simple_policy.allocation.types import AllocatePolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.allocation.utility import create_allocate_policy
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingPolicy
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import ScalingPolicyStrategy
from scaler.scheduler.controllers.policies.simple_policy.scaling.utility import create_scaling_policy
from scaler.scheduler.controllers.policies.types import PolicyEngineType


def create_policy(policy_engine_type: str, policy_content: str) -> Tuple[TaskAllocatePolicy, ScalingPolicy]:
    engine_type = PolicyEngineType(policy_engine_type)

    if engine_type == PolicyEngineType.SIMPLE:
        policy_kv = {
            k.strip(): v.strip() for item in policy_content.split(";") if "=" in item for k, v in [item.split("=", 1)]
        }

        required_keys = {"allocate", "scaling"}
        if policy_kv.keys() != required_keys:
            raise ValueError(f"simple policy_content requires keys {required_keys}, got {set(policy_kv.keys())}")

        return (
            create_allocate_policy(AllocatePolicyStrategy(policy_kv["allocate"])),
            create_scaling_policy(ScalingPolicyStrategy(policy_kv["scaling"])),
        )

    raise ValueError(f"Unknown policy_engine_type: {policy_engine_type}")
