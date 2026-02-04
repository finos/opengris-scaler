import enum

from scaler.scheduler.controllers.policies.simple_policy.allocation.capability_allocate_policy import (
    CapabilityAllocatePolicy,
)
from scaler.scheduler.controllers.policies.simple_policy.allocation.even_load_allocate_policy import (
    EvenLoadAllocatePolicy,
)


class AllocatePolicy(enum.Enum):
    even = EvenLoadAllocatePolicy
    capability = CapabilityAllocatePolicy

    def __str__(self):
        return self.name
