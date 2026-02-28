import dataclasses
import enum


@dataclasses.dataclass(frozen=True)
class WaterfallRule:
    """A single rule in the waterfall config, parsed from 'priority,adapter_id,max_workers'."""

    priority: int
    worker_adapter_id: bytes
    max_workers: int


class AdvanceScalingControllerStrategy(enum.Enum):
    WATERFALL = "waterfall"

    def __str__(self):
        return self.name
