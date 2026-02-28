from scaler.scheduler.controllers.policies.mixins import ScalerPolicy


def create_scaler_policy(policy_engine_type: str, policy_content: str) -> ScalerPolicy:
    parts = {k.strip(): v.strip() for item in policy_content.split(";") if "=" in item for k, v in [item.split("=", 1)]}

    if policy_engine_type == "simple":
        from scaler.scheduler.controllers.policies.simple_policy.simple_policy import SimplePolicy

        return SimplePolicy(parts)

    if policy_engine_type == "advance":
        from scaler.scheduler.controllers.policies.advance_policy.advance_policy import AdvancePolicy

        return AdvancePolicy(parts, policy_content)

    raise ValueError(f"Unknown policy type: {policy_engine_type}")
