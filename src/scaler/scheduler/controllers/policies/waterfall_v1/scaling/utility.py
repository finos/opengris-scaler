from typing import List

from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule


def parse_waterfall_rules(policy_content: str) -> List[WaterfallRule]:
    """Parse waterfall rules from policy_content.

    Expected format (one rule per line, ``#`` comments supported)::

        #priority,worker_manager_id[,max_task_concurrency[,min_task_concurrency]]
        1,native,8,8
        2,ecs,20
        3,orb,,2

    When ``max_task_concurrency`` is omitted or empty, the manager's heartbeat-reported capacity is used.
    When ``min_task_concurrency`` is omitted, the floor is 0.

    Raises ``ValueError`` on malformed input.
    """
    rules: List[WaterfallRule] = []
    for line_number, raw_line in enumerate(policy_content.splitlines(), start=1):
        # Strip inline comments
        line = raw_line.split("#", 1)[0].strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) not in (2, 3, 4):
            raise ValueError(
                f"waterfall_v1 policy_content line {line_number}: "
                "expected 'priority,worker_manager_id[,max_task_concurrency[,min_task_concurrency]]', "
                f"got {raw_line.strip()!r}"
            )

        raw_priority, worker_manager_id = parts[0], parts[1]
        raw_max_task_concurrency = parts[2] if len(parts) >= 3 else ""
        raw_min_task_concurrency = parts[3] if len(parts) == 4 else "0"

        if not worker_manager_id:
            raise ValueError(f"waterfall_v1 policy_content line {line_number}: worker_manager_id cannot be empty")

        max_task_concurrency = int(raw_max_task_concurrency) if raw_max_task_concurrency else None
        min_task_concurrency = int(raw_min_task_concurrency)
        if min_task_concurrency < 0:
            raise ValueError(f"waterfall_v1 policy_content line {line_number}: min_task_concurrency cannot be negative")
        if max_task_concurrency is not None and min_task_concurrency > max_task_concurrency:
            raise ValueError(
                f"waterfall_v1 policy_content line {line_number}: "
                "min_task_concurrency cannot exceed max_task_concurrency"
            )

        rules.append(
            WaterfallRule(
                priority=int(raw_priority),
                worker_manager_id=worker_manager_id.encode(),
                max_task_concurrency=max_task_concurrency,
                min_task_concurrency=min_task_concurrency,
            )
        )

    if not rules:
        raise ValueError("waterfall_v1 policy_content: no rules specified")

    seen: set = set()
    for rule in rules:
        if rule.worker_manager_id in seen:
            raise ValueError(f"waterfall_v1 policy_content: duplicate worker_manager_id {rule.worker_manager_id!r}")
        seen.add(rule.worker_manager_id)

    return rules
