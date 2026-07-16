"""Backend-agnostic scaling scenarios.

Each is a ``(test_case, deployment)`` function that drives a real client workload and asserts on how the
pool scaled, reading everything backend-specific (task callables, timing, pool observation) off the
``Deployment``. Written once here; every backend reuses them.

Each reports its numbers before asserting, so a failing run says what actually happened.
"""

from __future__ import annotations

import contextlib
import threading
import time

from scaler import Client
from tests.integration.framework import Deployment, report

# A concurrency-1 baseline for rising_load: submitted one at a time, so a single unit can serve them all.
_TRICKLE_TASKS = 4


class _PoolSample:
    def __init__(self) -> None:
        self.peak = 0
        self.created: set = set()

    def observe(self, names) -> None:
        self.peak = max(self.peak, len(names))
        self.created.update(names)


@contextlib.contextmanager
def _sampling(deployment: Deployment):
    """Watch the pool in the background while a load runs, recording peak concurrent units and every unit
    name seen (each provisioned unit gets a fresh name, so the set counts units CREATED over the run)."""
    sample = _PoolSample()
    stop = threading.Event()

    def poll() -> None:
        while not stop.is_set():
            sample.observe(deployment.running())
            time.sleep(deployment.profile.poll)

    thread = threading.Thread(target=poll, daemon=True)
    thread.start()
    try:
        yield sample
    finally:
        stop.set()
        thread.join(timeout=5.0)


def burst_and_drain(test_case, deployment: Deployment) -> None:
    """A held burst brings the pool up and returns correct results; an idle queue then drains it back to
    zero. Correct results prove real workers ran the work; the container count proves the scaling."""
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        with _sampling(deployment) as sample:
            futures = [client.submit(deployment.tasks.square, value) for value in range(profile.burst_tasks)]
            deadline = time.monotonic() + profile.scale_timeout
            while time.monotonic() < deadline and not all(future.done() for future in futures):
                time.sleep(profile.poll)
            results = [future.result() for future in futures]

    deadline = time.monotonic() + profile.drain_timeout
    while time.monotonic() < deadline and deployment.running():
        time.sleep(1.0)
    still_running = deployment.running()

    report(
        f"burst_and_drain: {len(results)} tasks ran on {len(sample.created)} unit(s), pool peaked at "
        f"{sample.peak} at once, drained to {len(still_running)} when idle"
    )
    test_case.assertEqual(results, expected, "the pool returned wrong results")
    test_case.assertGreaterEqual(sample.peak, 1, "no worker unit came up under load")
    test_case.assertEqual(still_running, [], f"pool did not drain to zero when idle ({still_running} still up)")


def rising_load(test_case, deployment: Deployment) -> None:
    """Capacity tracks demand: a sustained deep burst runs more units AT ONCE than a concurrency-1 trickle.

    Peak CONCURRENT units is the honest measure -- counting distinct names over the run would let a pool
    that only ever re-provisions one unit at a time pass by churning. The burst is held continuously
    because a single wave can drain before a second unit finishes booting, so a backend that genuinely
    scales reaches a higher peak while one that thrashes back to a single unit legitimately fails here.
    """
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        with _sampling(deployment) as trickle:
            for value in range(_TRICKLE_TASKS):
                test_case.assertEqual(client.submit(deployment.tasks.square, value).result(), value * value)

        with _sampling(deployment) as burst:
            deadline = time.monotonic() + profile.scale_timeout
            while burst.peak <= trickle.peak and time.monotonic() < deadline:
                test_case.assertEqual(client.map(deployment.tasks.square, range(profile.burst_tasks)), expected)

    report(f"rising_load: trickle peaked at {trickle.peak} unit(s), sustained burst peaked at {burst.peak}")
    test_case.assertGreater(
        burst.peak, trickle.peak, "a sustained burst did not run more units at once than a concurrency-1 trickle"
    )


def steady_load_stable(test_case, deployment: Deployment) -> None:
    """A sustained, non-varying load should settle on a stable pool -- about as many units created over the
    run as ever ran at once -- not thrash through provision and teardown.

    Steady-load only: do NOT reuse on scenarios that scale up AND down, which legitimately create > peak.
    """
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    with _sampling(deployment) as sample:
        with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
            deadline = time.monotonic() + profile.churn_window
            while time.monotonic() < deadline:
                wave = client.map(deployment.tasks.square, range(profile.burst_tasks))  # back-to-back waves
                test_case.assertEqual(wave, expected, "the pool returned wrong results under sustained load")

    created = len(sample.created)
    report(f"steady_load_stable: created {created} unit(s) over {profile.churn_window:.0f}s, peak {sample.peak}")
    test_case.assertGreaterEqual(created, 1, "no unit was provisioned under the steady load")
    test_case.assertLessEqual(
        created,
        sample.peak + profile.churn_tolerance,
        f"pool thrashed: created {created} units but only {sample.peak} ever ran at once (a steady load "
        f"should settle on a stable pool, not repeatedly provision and tear down)",
    )


def waterfall_spills(test_case, deployment: Deployment) -> None:
    """With managers at descending waterfall priority (the top one capped), a continuously replenished
    backlog fills the top pool first and, once it saturates, spills onto the next tier -- so both backends
    run work under one scheduler.

    The backlog is held continuously rather than submitted as blocking waves, which would let the queue
    drain between waves and the lower tier's desired count fall back, so the spill is deterministic even
    when the lower tier boots slowly. ``top_ran`` is asserted too, so a mis-prioritized policy that runs a
    lower tier first is a red rather than a pass. Each tier is attributed by its own container pool, since
    the shipped provisioners set no machine id.
    """
    profile = deployment.profile
    top, *overflow = deployment.handles
    top_ran = False
    spilled = False

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        inflight = {}  # future -> expected result; kept topped up so the overflow load stays saturated

        def replenish() -> None:
            for value in range(profile.burst_tasks):
                inflight[client.submit(deployment.tasks.square, value)] = value * value

        replenish()
        deadline = time.monotonic() + profile.scale_timeout
        while not spilled and time.monotonic() < deadline:
            for future in [future for future in inflight if future.done()]:
                test_case.assertEqual(future.result(), inflight.pop(future))  # real workers ran the work
            if len(inflight) < profile.burst_tasks:
                replenish()
            top_ran = top_ran or bool(top.pool.running())
            spilled = any(handle.pool.running() for handle in overflow)
            time.sleep(profile.poll)
        for future, expected in list(inflight.items()):
            test_case.assertEqual(future.result(), expected)

    report(f"waterfall_spills: top tier {top.worker_manager_id} ran = {top_ran}, spilled to a lower tier = {spilled}")
    test_case.assertTrue(top_ran, "the top-priority pool never ran work before overflow spilled to a lower tier")
    test_case.assertTrue(spilled, "sustained overflow beyond the top pool's cap never spilled to a lower tier")
