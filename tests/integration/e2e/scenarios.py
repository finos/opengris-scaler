"""Backend-agnostic scaling scenarios: each is a function of ``(test_case, deployment)`` that drives a real
client workload and asserts on how the pool scaled, reading everything backend-specific (task callables,
timing, pool observation) off the ``Deployment``. Written once here; every backend reuses them.

How many managers a scenario needs (>1 only for the waterfall spill) lives in ``matrix.MIN_MANAGERS``, so
the wiring can skip a scenario on a deployment too small for it.
"""

from __future__ import annotations

import threading
import time

from scaler import Client
from tests.integration.e2e.framework import Deployment


def burst_and_drain(test_case, deployment: Deployment) -> None:
    """A held burst brings the pool up (peak >= 1) and returns correct results; an idle queue then drains it
    back to zero. Correct results prove real workers ran the work; the running-container count proves scaling."""
    profile = deployment.profile
    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        futures = [client.submit(deployment.tasks.square, value) for value in range(profile.held_load_tasks)]
        peak = 0
        deadline = time.monotonic() + profile.provision_timeout
        while time.monotonic() < deadline and not all(future.done() for future in futures):
            peak = max(peak, deployment.running())
            time.sleep(profile.poll)
        results = [future.result() for future in futures]

    test_case.assertEqual(results, [value * value for value in range(profile.held_load_tasks)])
    test_case.assertGreaterEqual(peak, 1, "no worker unit came up under load")

    deadline = time.monotonic() + profile.drain_timeout
    while time.monotonic() < deadline and deployment.running() > 0:
        time.sleep(1.0)
    drained = deployment.running()
    print(f"scaling: peak {peak} unit(s) -> {drained} when idle")
    test_case.assertEqual(drained, 0, f"pool did not drain to zero when idle (still {drained} running)")


def work_spreads(test_case, deployment: Deployment) -> None:
    """Work runs on >= 2 distinct units over the run (each tags itself), proving the pool is not pinned to a
    single machine. Distinct units are counted over the whole run, not a snapshot, so churn does not hide the
    result -- but for the same reason this proves only that more than one machine was USED, not that two ran
    AT ONCE (a 0<->1 churn passes it via serial re-provisioning). rising_load owns the concurrency claim."""
    profile = deployment.profile
    tags: set = set()
    expected = [value * value for value in range(profile.burst_tasks)]
    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
        deadline = time.monotonic() + profile.spread_timeout
        while len(tags) < 2 and time.monotonic() < deadline:
            batch = client.map(deployment.tasks.square_tagged, range(profile.burst_tasks))
            test_case.assertEqual([value for value, _tag in batch], expected)
            tags |= {tag for _value, tag in batch if tag}
    print(f"scaling: work ran across units {sorted(tags)}")
    test_case.assertGreaterEqual(len(tags), 2, f"work only ran on {tags or 'no tagged unit'}; expected >= 2 units")


def rising_load(test_case, deployment: Deployment) -> None:
    """Capacity tracks demand: a SUSTAINED deep burst runs more units AT ONCE than a concurrency-1 trickle.
    Sample the live pool during each phase and compare the peak concurrent counts -- an instantaneous count
    is honest about concurrency where accumulated distinct names (which churn inflates) are not, so a pool
    that only ever re-provisions one unit at a time cannot pass by churning. The burst is held continuously
    (a single short wave can drain before a second unit finishes booting) so a backend that genuinely scales
    reaches a higher peak, while one that thrashes back to one unit legitimately fails here."""
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]

    def run_with_peak_sampling(load) -> int:
        peak = [0]
        stop = threading.Event()

        def sample() -> None:
            while not stop.is_set():
                peak[0] = max(peak[0], deployment.running())
                time.sleep(profile.poll)

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        try:
            load(peak)
        finally:
            stop.set()
            sampler.join(timeout=5.0)
        return peak[0]

    with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:

        def trickle(_peak) -> None:
            for value in range(profile.warmup_tasks):
                result, _tag = client.submit(deployment.tasks.square_tagged, value).result()
                test_case.assertEqual(result, value * value)

        trickle_peak = run_with_peak_sampling(trickle)

        def burst(peak) -> None:
            deadline = time.monotonic() + profile.spread_timeout
            while peak[0] <= trickle_peak and time.monotonic() < deadline:
                batch = client.map(deployment.tasks.square_tagged, range(profile.burst_tasks))
                test_case.assertEqual([value for value, _tag in batch], expected)

        burst_peak = run_with_peak_sampling(burst)

    print(f"scaling: trickle peaked at {trickle_peak} unit(s), burst peaked at {burst_peak}")
    test_case.assertGreater(
        burst_peak, trickle_peak, "a sustained burst did not run more units concurrently than a concurrency-1 trickle"
    )


def steady_load_stable(test_case, deployment: Deployment) -> None:
    """Steady-load stability: a sustained, non-varying load should settle on a stable pool -- about as many
    units created over the run as ever run concurrently at the peak -- not thrash through provision/teardown.
    Steady-load only: do NOT reuse on scenarios that scale up AND down, which legitimately create > peak."""
    profile = deployment.profile
    expected = [value * value for value in range(profile.burst_tasks)]
    stop = threading.Event()
    peak = 0
    created: set = set()

    def sample() -> None:
        nonlocal peak
        while not stop.is_set():
            names = deployment.running_names()
            peak = max(peak, len(names))
            created.update(names)
            time.sleep(profile.churn_sample)

    sampler = threading.Thread(target=sample, daemon=True)
    sampler.start()
    try:
        with Client(deployment.harness.scheduler_address, timeout_seconds=profile.client_timeout) as client:
            deadline = time.monotonic() + profile.churn_window
            while time.monotonic() < deadline:
                wave = client.map(deployment.tasks.square, range(profile.burst_tasks))  # steady back-to-back waves
                test_case.assertEqual(wave, expected)  # sustained load must still compute correct results
    finally:
        stop.set()
        sampler.join(timeout=5.0)

    total = len(created)
    print(f"scaling churn: created {total} unit(s), peak {peak} concurrent")
    test_case.assertGreaterEqual(total, 1, "no unit was provisioned under the steady load")
    test_case.assertLessEqual(
        total,
        peak + profile.churn_tolerance,
        f"pool thrashed: created {total} units but only {peak} ran concurrently at the peak "
        f"(steady load should settle on a stable pool, not repeatedly provision and tear down)",
    )


def waterfall_spills(test_case, deployment: Deployment) -> None:
    """With managers at descending waterfall priority (the top one capped), a continuously replenished backlog
    fills the top pool first and, once it saturates, spills onto a lower tier -- so every backend runs work
    under one scheduler. The backlog is held continuously (not blocking per-wave maps, which let the queue
    drain between waves and the lower tier's desired fall back) so the top pool stays at its cap and the spill
    is deterministic even when the lower tier boots slowly. ``top_ran`` is asserted so a mis-prioritized policy
    that runs a lower tier before the top is a red, not a pass. Each tier is attributed by its own container
    pool, since the shipped provisioners set no id."""
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
        deadline = time.monotonic() + profile.spread_timeout
        while not spilled and time.monotonic() < deadline:
            for future in [future for future in inflight if future.done()]:
                test_case.assertEqual(future.result(), inflight.pop(future))  # real workers ran the work
            if len(inflight) < profile.burst_tasks:
                replenish()
            top_ran = top_ran or top.pool.running() > 0
            spilled = any(handle.pool.running() > 0 for handle in overflow)
            time.sleep(profile.poll)
        for future, expected in list(inflight.items()):
            test_case.assertEqual(future.result(), expected)
    print(f"waterfall: top tier ran = {top_ran}, spilled to lower tier = {spilled}")
    test_case.assertTrue(top_ran, "the top-priority pool never ran work before overflow spilled to a lower tier")
    test_case.assertTrue(spilled, "sustained overflow beyond the top pool's cap never spilled to a lower tier")
