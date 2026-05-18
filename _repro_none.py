"""Faithful repro: run test_none_value's EXACT graph vs a non-None control,
alternating, in a churn loop, with the accept+connect instrumented _ymq, to see
(a) whether only the None graph misbehaves, and (b) the accept-side [YMQDIAG]
timeline (does the OSS AcceptServer ever fire onConnection for the failing
connector? does it DISCONNECT mid-test?).

Usage: python _repro_none.py <iterations>
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


def func1(a):
    return a


NONE_GRAPH = ({"None": None, "a": 1, "b": (func, "a", "None")}, ["b"], {"b": 1})
CTRL_GRAPH = ({"z": 1, "a": 1, "b": (func, "a", "z")}, ["b"], {"b": 1})  # same shape, no None


def one_round(i: int, which: str):
    graph, keys, expected = NONE_GRAPH if which == "none" else CTRL_GRAPH
    t0 = time.time()
    combo = SchedulerClusterCombo(n_workers=3, event_loop="builtin")
    try:
        with Client(address=combo.get_address()) as client:
            res = client.get(graph, keys=keys)
            assert res == expected, f"{which}: {res}"
    finally:
        combo.shutdown()
    return time.time() - t0


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 30
    bad = 0
    for i in range(n):
        which = "none" if i % 2 == 0 else "ctrl"
        try:
            dt = one_round(i, which)
            tag = "SLOW" if dt > 25 else "ok"
            if dt > 25:
                bad += 1
            print(f">>> iter {i} [{which}]: {tag} {dt:.1f}s", flush=True)
        except BaseException as e:  # noqa
            print(f">>> iter {i} [{which}]: FAILED {type(e).__name__}: {e}", flush=True)
            bad += 1
    print(f">>> NONE-REPRO DONE: {bad} slow/failed of {n}", flush=True)
    sys.exit(1 if bad else 0)
