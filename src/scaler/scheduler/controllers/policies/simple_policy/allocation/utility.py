from scaler.scheduler.controllers.policies.simple_policy.allocation.capability_allocate_policy import (
    CapabilityAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.even_load_allocate_policy import (
    EvenLoadAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.mixins import TaskAllocatePolicy
from scaler.scheduler.controllers.policies.simple_policy.allocation.types import AllocatePolicyStrategy


def create_allocate_policy(allocate_policy_strategy: AllocatePolicyStrategy) -> TaskAllocatePolicy:
    if allocate_policy_strategy == AllocatePolicyStrategy.CAPABILITY:
        return CapabilityAllocatePolicy()
    elif allocate_policy_strategy == AllocatePolicyStrategy.EVEN_LOAD:
        return EvenLoadAllocatePolicy()

    raise ValueError(f"unsupported allocate policy strategy: {allocate_policy_strategy}")
