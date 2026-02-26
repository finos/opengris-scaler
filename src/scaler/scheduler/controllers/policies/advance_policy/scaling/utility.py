from scaler.scheduler.controllers.policies.advance_policy.scaling.types import (
    AdvanceScalingControllerStrategy,
    WaterfallRule,
)
from scaler.scheduler.controllers.policies.advance_policy.scaling.waterfall import WaterfallScalingController
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController


def _parse_waterfall_rules(scaling_config: str) -> list:
    """Parse waterfall rules from semicolon-separated 'priority,adapter_id,max_workers' items."""
    rules = []
    for item in scaling_config.split(";"):
        item = item.strip()
        if not item or "=" in item:
            continue
        parts = [p.strip() for p in item.split(",")]
        if len(parts) == 3:
            rules.append(
                WaterfallRule(priority=int(parts[0]), worker_adapter_id=parts[1].encode(), max_workers=int(parts[2]))
            )
    return rules


def create_advance_scaling_controller(
    strategy: AdvanceScalingControllerStrategy, scaling_config: str
) -> ScalingController:
    if strategy == AdvanceScalingControllerStrategy.WATERFALL:
        rules = _parse_waterfall_rules(scaling_config)
        return WaterfallScalingController(rules)

    raise ValueError(f"unsupported advance scaling controller strategy: {strategy}")
