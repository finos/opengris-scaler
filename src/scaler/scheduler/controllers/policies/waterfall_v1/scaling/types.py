import dataclasses


@dataclasses.dataclass(frozen=True)
class WaterfallRule:
    """A single rule in the waterfall config, parsed from 'priority,worker_manager_id,max_task_concurrency'."""

    priority: int
    worker_manager_id: bytes
    max_task_concurrency: int
