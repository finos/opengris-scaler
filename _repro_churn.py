"""Faithful reproduction of the real CI scenario: consecutive TestGraph tests
each create SchedulerClusterCombo(n_workers=3) in setUp, run a tiny graph, and
shut it down in tearDown -- back to back, so on Windows the previous combo's
TerminateProcess teardown overlaps the next combo's spawn storm.

We run test_none_value's exact workload in a tight loop and watch for a slow /
failing iteration. The instrumented _ymq logs `[YMQDIAG] connect attempt failed
... uv=<ERR>` on every failed connect, so a reproduction pinpoints the cause.

Usage: python _repro_churn.py <iterations>
"""

import faulthandler
import sys
import time

faulthandler.enable()
faulthandler.dump_traceback_later(90, repeat=True, exit=False)

from scaler import Client, SchedulerClusterCombo  # noqa: E402
from scaler.utility.logging.utility import setup_logger  # noqa: E402

setup_logger()


def func(a, optional_param_can_be_none):
    return a


def one_round(i: int) -> float:
    t0 = time.time()
    combo = SchedulerClusterCombo(n_workers=3, event_loop="builtin")
    try:
        with Client(address=combo.get_address()) as client:
            graph = {"None": None, "a": 1, "b": (func, "a", "None")}
            results = client.get(graph, keys=["b"])
            assert results == {"b": 1}, results
    finally:
        combo.shutdown()
    return time.time() - t0


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    slow = 0
    for i in range(n):
        try:
            dt = one_round(i)
            tag = "SLOW" if dt > 25 else "ok"
            if dt > 25:
                slow += 1
            print(f">>> iter {i}: {tag} {dt:.1f}s", flush=True)
        except BaseException as e:  # noqa
            print(f">>> iter {i}: FAILED {type(e).__name__}: {e}", flush=True)
            slow += 1
    print(f">>> CHURN DONE: {slow} slow/failed of {n}", flush=True)
    sys.exit(1 if slow else 0)
