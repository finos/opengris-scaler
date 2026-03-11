from typing import List

from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule


def parse_waterfall_rules(policy_content: str) -> List[WaterfallRule]:
    """Parse waterfall rules from policy_content.

    Expected format (one rule per line, ``#`` comments supported)::

        #priority,worker_type,max_task_concurrency
        1,native,10
        2,ecs,20

    Raises ``ValueError`` on malformed input.
    """
    rules: List[WaterfallRule] = []
    for line_number, raw_line in enumerate(policy_content.splitlines(), start=1):
        # Strip inline comments
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) != 3:
            raise ValueError(
                f"waterfall_v1 policy_content line {line_number}: "
                f"expected 'priority,worker_type,max_task_concurrency', got {raw_line.strip()!r}"
            )

        raw_priority, worker_type, raw_max_task_concurrency = parts

        if not worker_type:
            raise ValueError(f"waterfall_v1 policy_content line {line_number}: worker_type cannot be empty")

        rules.append(
            WaterfallRule(
                priority=int(raw_priority),
                worker_type=worker_type.encode(),
                max_task_concurrency=int(raw_max_task_concurrency),
            )
        )

    if not rules:
        raise ValueError("waterfall_v1 policy_content: no rules specified")

    return rules
