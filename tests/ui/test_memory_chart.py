"""The memory and CPU chart: timed samples the browser scrolls itself, and axes that scale to what the fleet uses."""

import datetime
import json
import unittest
from typing import Any, Dict, Optional

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import (
    BinderStatus,
    ClientManagerStatus,
    ObjectManagerStatus,
    Resource,
    ScalingManagerStatus,
    StateScheduler,
    TaskManagerStatus,
    WorkerManagerStatus,
)
from scaler.ui.app import (
    CPU_CHART_MINIMUM_PERCENT,
    MEMORY_CHART_TICKS,
    BrowserView,
    MemoryChartState,
    WebGUIConfig,
    WebUIApp,
    _cpu_axis_ticks,
)

WINDOW_SECONDS = 300


def status_frame() -> StateScheduler:
    return StateScheduler.from_bytes(
        StateScheduler(
            binder=BinderStatus(received=[], sent=[]),
            scheduler=Resource(cpu=0, rss=0),
            rssFree=0,
            clientManager=ClientManagerStatus(clients=[]),
            objectManager=ObjectManagerStatus(numberOfObjects=0),
            taskManager=TaskManagerStatus(stateToCount=[]),
            workerManager=WorkerManagerStatus(workers=[]),
            scalingManager=ScalingManagerStatus(managedWorkers=[], workerManagerDetails=[]),
        ).to_bytes()
    )


def next_payload(app: WebUIApp, browser_id: int) -> Dict[str, Any]:
    """Run one batcher tick and return what it queued for the browser."""
    app._batch_once()
    stream = app.get_browser(browser_id)
    assert stream is not None
    payload: Optional[str] = stream.take(timeout=1)
    assert payload is not None
    return json.loads(payload)


class TestCPUAxis(unittest.TestCase):
    def test_the_axis_rounds_up_to_a_round_step(self) -> None:
        for peak, maximum in ((37.0, 40.0), (130.0, 200.0), (400.0, 400.0), (401.0, 800.0), (6400.0, 8000.0)):
            with self.subTest(peak=peak):
                ticks = _cpu_axis_ticks(peak)
                self.assertEqual(ticks[-1], maximum)
                self.assertEqual(len(ticks), MEMORY_CHART_TICKS, "one tick per memory gridline")

    def test_ticks_are_evenly_spaced_from_zero(self) -> None:
        self.assertEqual(_cpu_axis_ticks(130.0), [0, 50, 100, 150, 200])

    def test_an_idle_fleet_reads_against_the_floor(self) -> None:
        """The few percent an idle fleet's agents use must not fill the plot."""
        self.assertEqual(_cpu_axis_ticks(0.0)[-1], CPU_CHART_MINIMUM_PERCENT)
        self.assertEqual(_cpu_axis_ticks(3.0), [0, 2.5, 5, 7.5, 10])

    def test_the_chart_labels_its_cpu_axis_from_the_samples_in_its_window(self) -> None:
        chart = MemoryChartState()
        for cpu_percent in (20.0, 130.0, 60.0):
            chart.record_fleet_sample(rss_bytes=1_000_000, cpu_percent=cpu_percent)

        ticks = chart.get_render_data(window_seconds=WINDOW_SECONDS, scale="log", since=None)["cpu_ticks"]
        self.assertEqual([tick["label"] for tick in ticks], ["0%", "50%", "100%", "150%", "200%"])


class TestMemorySamples(unittest.TestCase):
    def test_a_sample_carries_the_time_it_was_taken(self) -> None:
        """The browser places a sample against the clock, so it must not be an offset from when it was sent."""
        chart = MemoryChartState()
        before = datetime.datetime.now().timestamp()
        chart.record_fleet_sample(rss_bytes=5_000, cpu_percent=12.34)

        data = chart.get_render_data(window_seconds=WINDOW_SECONDS, scale="linear", since=None)
        [[taken, rss_bytes, cpu_percent]] = data["samples"]
        self.assertAlmostEqual(taken, before, delta=1)
        self.assertEqual((rss_bytes, cpu_percent), (5_000, 12.3))
        self.assertAlmostEqual(data["now"], taken, delta=1)
        self.assertFalse(data["append"])

    def test_since_sends_only_newer_samples_against_axes_for_the_whole_window(self) -> None:
        chart = MemoryChartState()
        chart.record_fleet_sample(rss_bytes=3_000_000_000, cpu_percent=900.0)
        since = chart.latest_sample_time()
        chart.record_fleet_sample(rss_bytes=1_000, cpu_percent=1.0)

        data = chart.get_render_data(window_seconds=WINDOW_SECONDS, scale="linear", since=since)
        self.assertTrue(data["append"])
        self.assertEqual([sample[1] for sample in data["samples"]], [1_000])
        self.assertEqual(data["y_ticks"][-1]["val"], 3_000_000_000, "the older, bigger sample still sets the axis")
        self.assertEqual(data["cpu_ticks"][-1]["val"], 1000)

    def test_a_sample_older_than_the_window_is_not_sent(self) -> None:
        chart = MemoryChartState()
        now = datetime.datetime.now().timestamp()
        chart._live.extend([(now - WINDOW_SECONDS - 1, 1, 0.0), (now - 1, 2, 0.0)])

        data = chart.get_render_data(window_seconds=WINDOW_SECONDS, scale="linear", since=None)
        self.assertEqual([sample[1] for sample in data["samples"]], [2])


class TestMemoryChartStream(unittest.TestCase):
    def test_a_tick_sends_the_browser_only_the_samples_it_took(self) -> None:
        app = WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))
        browser = app.add_browser(BrowserView())
        app._on_monitor_message(status_frame())
        app._batch_once()
        browser.take(timeout=1)

        app._on_monitor_message(status_frame())
        chart = next_payload(app, browser.browser_id)["memory_chart"]
        self.assertTrue(chart["append"])
        self.assertEqual(len(chart["samples"]), 1)

        self.assertNotIn("memory_chart", next_payload(app, browser.browser_id), "no new sample, nothing to send")

    def test_a_full_state_sends_the_whole_window(self) -> None:
        app = WebUIApp(WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380")))
        for _ in range(3):
            app._on_monitor_message(status_frame())
            app._batch_once()

        chart = app.get_full_state(BrowserView())["memory_chart"]
        self.assertFalse(chart["append"])
        self.assertEqual(len(chart["samples"]), 3)


if __name__ == "__main__":
    unittest.main()
