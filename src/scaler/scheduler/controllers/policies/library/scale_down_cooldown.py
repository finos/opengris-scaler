import time
from typing import Dict, FrozenSet, List, Optional, Tuple

DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS = 30


class ScaleDownCooldown:
    """
    Anti-flapping gate for scale-down decisions belonging to a single worker manager.

    A caller should own one instance per worker manager, since one manager's scale-up
    traffic must never reset a different manager's independent scale-down cooldown.

    Two entry points cover the two ways scaling policies present a "desired" worker count:

    - apply(desired, current): for policies with a real, currently-connected worker count
      to compare against (e.g. len(managed_worker_ids)). A single scalar decision.
    - reconcile(desired_per_capset): for declarative per-capability-set policies with no
      independent "how many workers are really running for this capset" signal. Gates each
      capset against its own last-emitted value, and treats a capset that disappears from
      the input entirely (its tasks emptied out) the same as it being recomputed to 0.

    Both share the same cooldown_seconds but keep independent state, so one instance can
    safely serve both a policy's generic entry (via apply) and its capability-specific
    entries (via reconcile).
    """

    def __init__(self, cooldown_seconds: float = DEFAULT_SCALE_DOWN_COOLDOWN_SECONDS):
        self._cooldown_seconds = cooldown_seconds

        self._scale_down_since: Optional[float] = None

        self._per_capset_since: Dict[FrozenSet[str], float] = {}
        self._held_per_capset: Dict[FrozenSet[str], Tuple[Dict[str, int], int]] = {}

    def apply(self, desired: int, current: int) -> int:
        """Gate a single desired count against the real current count."""
        if desired >= current:
            self._scale_down_since = None
            return desired

        now = time.time()
        if self._scale_down_since is None:
            self._scale_down_since = now

        if now - self._scale_down_since < self._cooldown_seconds:
            return current

        self._scale_down_since = None
        return desired

    def reconcile(self, desired_per_capset: List[Tuple[Dict[str, int], int]]) -> List[Tuple[Dict[str, int], int]]:
        """Gate a declarative per-capset list against each capset's own last-held value."""
        current = {
            frozenset(capabilities.keys()): (capabilities, desired) for capabilities, desired in desired_per_capset
        }

        # Fetched lazily: only capsets on a scale-down path need "now", and all of them
        # observe the same instant within a single reconcile() call.
        now: Optional[float] = None

        result: List[Tuple[Dict[str, int], int]] = []
        for capset in set(current) | set(self._held_per_capset):
            held_capabilities, held_value = self._held_per_capset.get(capset, current.get(capset, ({}, 0)))
            capabilities, desired = current.get(capset, (held_capabilities, 0))

            if desired >= held_value:
                self._per_capset_since.pop(capset, None)
                self._set_or_forget_capset(capset, capabilities, desired, result)
                continue

            if now is None:
                now = time.time()

            since = self._per_capset_since.get(capset)
            if since is None:
                self._per_capset_since[capset] = now
                result.append((held_capabilities, held_value))
                continue

            if now - since < self._cooldown_seconds:
                result.append((held_capabilities, held_value))
                continue

            self._per_capset_since.pop(capset, None)
            self._set_or_forget_capset(capset, capabilities, desired, result)

        return result

    def _set_or_forget_capset(
        self,
        capset: FrozenSet[str],
        capabilities: Dict[str, int],
        desired: int,
        result: List[Tuple[Dict[str, int], int]],
    ) -> None:
        if desired > 0:
            self._held_per_capset[capset] = (capabilities, desired)
            result.append((capabilities, desired))
        else:
            self._held_per_capset.pop(capset, None)
