import asyncio
import unittest
from unittest import mock

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import HistogramDataPoint, InMemoryMetricReader

from scaler.scheduler.metrics import EventLoopLagMonitor
from scaler.utility.semantic_conventions import SCHEDULER_EVENT_LOOP_LAG_METRIC

# a short sample keeps the test fast; its oversleep is a few milliseconds on an idle loop
SAMPLE_SECONDS = 0.01

# a few milliseconds of lag must fall in a bucket at least this narrow for quantiles to mean anything
MAX_BUCKET_UPPER_BOUND_SECONDS = 0.1


def lag_data_point(reader: InMemoryMetricReader) -> HistogramDataPoint:
    for resource_metrics in reader.get_metrics_data().resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == SCHEDULER_EVENT_LOOP_LAG_METRIC:
                    data_point = metric.data.data_points[0]
                    assert isinstance(data_point, HistogramDataPoint)
                    return data_point
    raise AssertionError(f"no {SCHEDULER_EVENT_LOOP_LAG_METRIC} data point")


class TestEventLoopLagMonitor(unittest.TestCase):
    def test_millisecond_lag_lands_in_a_sub_second_bucket(self) -> None:
        """The lag histogram's buckets resolve milliseconds, so a few ms of lag does not share a bucket with 5 s."""
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        self.addCleanup(provider.shutdown)

        with (
            mock.patch.object(metrics, "get_meter", provider.get_meter),
            mock.patch("scaler.scheduler.metrics.EVENT_LOOP_LAG_SAMPLE_SECONDS", SAMPLE_SECONDS),
        ):
            monitor = EventLoopLagMonitor()
            asyncio.run(monitor.routine())

        data_point = lag_data_point(reader)
        bucket_i = next(count_i for count_i, count in enumerate(data_point.bucket_counts) if count)
        upper_bounds = list(data_point.explicit_bounds) + [float("inf")]
        self.assertLessEqual(upper_bounds[bucket_i], MAX_BUCKET_UPPER_BOUND_SECONDS)
