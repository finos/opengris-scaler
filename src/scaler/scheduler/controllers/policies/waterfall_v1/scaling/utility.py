from typing import List

from scaler.scheduler.controllers.policies.waterfall_v1.scaling.types import WaterfallRule


def parse_waterfall_rules(policy_content: str) -> List[WaterfallRule]:
    """Parse waterfall rules from policy_content.

    Expected format (one rule per line, ``#`` comments supported)::

        #priority,adapter_id,max_workers
        1,adapter_a,10
        2,adapter_b,20

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
                f"waterfall_v1 policy_content line {line_number}: expected 'priority,adapter_id,max_workers', "
                f"got {raw_line.strip()!r}"
            )

        raw_priority, adapter_id, raw_max_workers = parts

        if not adapter_id:
            raise ValueError(f"waterfall_v1 policy_content line {line_number}: adapter_id cannot be empty")

        rules.append(
            WaterfallRule(
                priority=int(raw_priority),
                worker_manager_adapter_id=adapter_id.encode(),
                max_workers=int(raw_max_workers),
            )
        )

    if not rules:
        raise ValueError("waterfall_v1 policy_content: no rules specified")

    return rules
