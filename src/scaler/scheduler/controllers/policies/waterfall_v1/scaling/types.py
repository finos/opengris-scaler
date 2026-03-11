import dataclasses


@dataclasses.dataclass(frozen=True)
class WaterfallRule:
    """A single rule in the waterfall config, parsed from 'priority,worker_type,max_task_concurrency'."""

    priority: int
    worker_type: bytes
    max_task_concurrency: int
