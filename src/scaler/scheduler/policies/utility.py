from typing import Dict, List, Optional, Tuple

from scaler.scheduler.policies.allocation.capability import CapabilityAllocatePolicy
from scaler.scheduler.policies.allocation.even_load import EvenLoadAllocatePolicy
from scaler.scheduler.policies.allocation.mixins import TaskAllocatePolicy
from scaler.scheduler.policies.scaling.capability import CapabilityScalingPolicy
from scaler.scheduler.policies.scaling.mixins import ScalingPolicy
from scaler.scheduler.policies.scaling.static import StaticScalingPolicy
from scaler.scheduler.policies.scaling.types import WorkerManagerBounds
from scaler.scheduler.policies.scaling.vanilla import VanillaScalingPolicy
from scaler.scheduler.policies.scaling.waterfall import WaterfallScalingPolicy, parse_waterfall_rules
from scaler.scheduler.policies.types import AllocatePolicyStrategy, PolicyEngineType, ScalingPolicyStrategy
from scaler.utility.identifiers import WorkerManagerID

_WATERFALL_V1_ALLOCATE_POLICY = AllocatePolicyStrategy.CAPABILITY


def create_allocate_policy(allocate_policy_strategy: AllocatePolicyStrategy) -> TaskAllocatePolicy:
    if allocate_policy_strategy == AllocatePolicyStrategy.CAPABILITY:
        return CapabilityAllocatePolicy()
    elif allocate_policy_strategy == AllocatePolicyStrategy.EVEN_LOAD:
        return EvenLoadAllocatePolicy()

    raise ValueError(f"unsupported allocate policy strategy: {allocate_policy_strategy}")


def create_scaling_policy(scaling: str) -> ScalingPolicy:
    """Build a scaling policy from its `name[:argument]` string, or `name[entries]`.

    Only `static` takes an argument, and only `static` and `vanilla` take entries:

        static                          every worker manager runs its advertised maximum
        static:4                        every worker manager runs 4, capped at its advertised maximum
        static[native:4, ecs:0]         native runs 4, ecs runs 0, any other runs its advertised maximum
        vanilla                         every worker manager scales between 0 and its advertised maximum
        vanilla[native:8:8, ecs:100]    native holds 8, ecs scales between 0 and 100
        capability                      scales per capability group

    An argument and entries cannot be combined: `static:2[native:4]` is refused."""
    name, argument, entries = _split_scaling(scaling)
    match (_parse_strategy(name), argument, entries):
        case (ScalingPolicyStrategy.STATIC, None, None):
            return StaticScalingPolicy(None, {})
        case (ScalingPolicyStrategy.STATIC, str() as argument, None):
            return StaticScalingPolicy(_parse_count(argument.strip(), f"scaling {scaling!r}"), {})
        case (ScalingPolicyStrategy.STATIC, None, str() as entries):
            return StaticScalingPolicy(None, _parse_static_task_concurrencies(entries))
        case (ScalingPolicyStrategy.VANILLA, None, None):
            return VanillaScalingPolicy({})
        case (ScalingPolicyStrategy.VANILLA, None, str() as entries):
            return VanillaScalingPolicy(_parse_vanilla_bounds(entries))
        case (ScalingPolicyStrategy.CAPABILITY, None, None):
            return CapabilityScalingPolicy()
        case (_, str(), str()):
            raise ValueError(f"scaling takes an argument or entries, not both, got {scaling!r}")
        case _:
            refused_part = "argument" if argument is not None else "entries"
            raise ValueError(f"scaling policy {name!r} takes no {refused_part}, got {scaling!r}")


def _split_scaling(scaling: str) -> Tuple[str, Optional[str], Optional[str]]:
    """Split `name`, `name:argument`, or `name[entries]` into the name, the argument, and the entries.

    A part the string leaves out is None, so `static:` has an empty argument rather than none."""
    head, bracket, tail = scaling.strip().partition("[")
    entries = None
    if bracket:
        if not tail.endswith("]"):
            raise ValueError(f"scaling must be 'name', 'name:argument', or 'name[entries]', got {scaling!r}")
        entries = tail[:-1]

    name, colon, argument = head.partition(":")
    return name.strip(), argument if colon else None, entries


def _parse_strategy(name: str) -> ScalingPolicyStrategy:
    try:
        return ScalingPolicyStrategy(name)
    except ValueError:
        known_names = ", ".join(strategy.value for strategy in ScalingPolicyStrategy)
        raise ValueError(f"unknown scaling policy {name!r}, expected one of {known_names}") from None


def _parse_static_task_concurrencies(entries: str) -> Dict[WorkerManagerID, int]:
    """Parse `worker_manager_id:task_concurrency` entries separated by `,`."""
    fields_by_manager = _split_entries(entries, "worker_manager_id:task_concurrency", (1,))
    return {
        WorkerManagerID.from_string(worker_manager_id): _parse_count(
            task_concurrency, f"static entry {worker_manager_id!r}"
        )
        for worker_manager_id, (task_concurrency,) in fields_by_manager.items()
    }


def _parse_vanilla_bounds(entries: str) -> Dict[WorkerManagerID, WorkerManagerBounds]:
    """Parse `worker_manager_id:max_task_concurrency[:min_task_concurrency]` entries separated by `,`.

    An empty max_task_concurrency leaves the cap to the manager's advertised maximum."""
    fields_by_manager = _split_entries(entries, "worker_manager_id:max_task_concurrency[:min_task_concurrency]", (1, 2))
    bounds: Dict[WorkerManagerID, WorkerManagerBounds] = {}
    for worker_manager_id, fields in fields_by_manager.items():
        context = f"vanilla bounds entry {worker_manager_id!r}"
        max_task_concurrency = _parse_count(fields[0], context) if fields[0] else None
        min_task_concurrency = _parse_count(fields[1], context) if len(fields) == 2 else 0
        if max_task_concurrency is not None and min_task_concurrency > max_task_concurrency:
            raise ValueError(f"{context}: the minimum exceeds the maximum")

        bounds[WorkerManagerID.from_string(worker_manager_id)] = WorkerManagerBounds(
            max_task_concurrency, min_task_concurrency
        )

    return bounds


def _split_entries(entries: str, entry_format: str, field_counts: Tuple[int, ...]) -> Dict[str, List[str]]:
    """Split `,`-separated `worker_manager_id:field...` entries into the fields after the id, keyed by the id."""
    fields_by_manager: Dict[str, List[str]] = {}
    for entry in entries.split(","):
        worker_manager_id, *fields = [field.strip() for field in entry.split(":")]
        has_bracket = "[" in worker_manager_id or "]" in worker_manager_id
        if len(fields) not in field_counts or not worker_manager_id or has_bracket:
            raise ValueError(f"scaling entry must be '{entry_format}', got {entry.strip()!r}")
        if worker_manager_id in fields_by_manager:
            raise ValueError(f"scaling entries name worker manager {worker_manager_id!r} twice")

        fields_by_manager[worker_manager_id] = fields

    return fields_by_manager


def _parse_count(field: str, context: str) -> int:
    # isdigit() alone accepts non-ASCII digits such as '\u00b2', which int() then refuses.
    if not (field.isascii() and field.isdigit()):
        raise ValueError(f"{context}: {field!r} is not a non-negative task concurrency")

    return int(field)


def create_policies(policy_engine_type: str, policy_content: str) -> Tuple[TaskAllocatePolicy, ScalingPolicy]:
    engine_type = PolicyEngineType(policy_engine_type)

    if engine_type == PolicyEngineType.SIMPLE:
        policy_kv = {
            k.strip(): v.strip() for item in policy_content.split(";") if "=" in item for k, v in [item.split("=", 1)]
        }

        required_keys = {"allocate", "scaling"}
        if policy_kv.keys() != required_keys:
            raise ValueError(f"simple policy_content requires keys {required_keys}, got {set(policy_kv.keys())}")

        return (
            create_allocate_policy(AllocatePolicyStrategy(policy_kv["allocate"])),
            create_scaling_policy(policy_kv["scaling"]),
        )

    if engine_type == PolicyEngineType.WATERFALL_V1:
        rules = parse_waterfall_rules(policy_content)
        return create_allocate_policy(_WATERFALL_V1_ALLOCATE_POLICY), WaterfallScalingPolicy(rules)

    raise ValueError(f"Unknown policy_engine_type: {policy_engine_type}")
