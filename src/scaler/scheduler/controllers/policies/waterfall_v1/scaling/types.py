import dataclasses


@dataclasses.dataclass(frozen=True)
class WaterfallRule:
    """A single rule in the waterfall config, parsed from 'priority,adapter_id,max_workers'."""

    priority: int
    worker_manager_adapter_id: bytes
    max_workers: int
