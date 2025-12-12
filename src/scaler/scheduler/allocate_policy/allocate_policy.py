import enum

from scaler.scheduler.allocate_policy.capability_allocate_policy import CapabilityAllocatePolicy
from scaler.scheduler.allocate_policy.even_load_allocate_policy import EvenLoadAllocatePolicy


class AllocatePolicy(enum.Enum):
    even = EvenLoadAllocatePolicy
    capability = CapabilityAllocatePolicy

    def __str__(self):
        return self.name
