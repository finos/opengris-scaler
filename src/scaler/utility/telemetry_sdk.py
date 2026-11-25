"""The OpenTelemetry SDK setup, imported only when export is configured: it needs the `otel` extra."""

import dataclasses
import importlib.util
import logging
import os
import socket
import uuid
from typing import Any, Dict, Optional, Sequence

from opentelemetry import baggage, metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.common._log_encoder import encode_logs
from opentelemetry.exporter.otlp.proto.common.metrics_encoder import encode_metrics
from opentelemetry.exporter.otlp.proto.common.trace_encoder import encode_spans
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler, ReadableLogRecord
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter, LogRecordExportResult
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExporter,
    MetricExportResult,
    MetricsData,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult

from scaler.about import __version__
from scaler.protocol.capnp import TelemetryBatch
from scaler.utility.semantic_conventions import SERVICE_NAMESPACE
from scaler.utility.telemetry_relay import TelemetryRelayBuffer

SCALER_LOGGER_NAME = "scaler"


class BaggageSpanProcessor(SpanProcessor):
    """Copies the baggage of a span's parent context onto the span as attributes, so a user tag reaches every span."""

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        for key, value in baggage.get_all(parent_context).items():
            span.set_attribute(key, str(value))


class RelaySpanExporter(SpanExporter):
    def __init__(self, relay_buffer: TelemetryRelayBuffer) -> None:
        self._relay_buffer = relay_buffer

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self._relay_buffer.put(TelemetryBatch.Signal.traces, encode_spans(spans).SerializeToString())
        return SpanExportResult.SUCCESS


class RelayMetricExporter(MetricExporter):
    def __init__(self, relay_buffer: TelemetryRelayBuffer) -> None:
        super().__init__()
        self._relay_buffer = relay_buffer

    def export(self, metrics_data: MetricsData, timeout_millis: float = 10_000, **kwargs: Any) -> MetricExportResult:
        self._relay_buffer.put(TelemetryBatch.Signal.metrics, encode_metrics(metrics_data).SerializeToString())
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs: Any) -> None:
        pass


class RelayLogExporter(LogRecordExporter):
    def __init__(self, relay_buffer: TelemetryRelayBuffer) -> None:
        self._relay_buffer = relay_buffer

    def export(self, batch: Sequence[ReadableLogRecord]) -> LogRecordExportResult:
        self._relay_buffer.put(TelemetryBatch.Signal.logs, encode_logs(batch).SerializeToString())
        return LogRecordExportResult.SUCCESS

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return True

    def shutdown(self) -> None:
        pass


@dataclasses.dataclass(frozen=True)
class InstalledSDK:
    tracer_provider: TracerProvider
    meter_provider: MeterProvider
    logger_provider: LoggerProvider

    def flush(self) -> None:
        """Hand every finished span, metric, and log record to the exporters now."""
        self.tracer_provider.force_flush()
        self.meter_provider.force_flush()
        self.logger_provider.force_flush()


def install_sdk(
    service_name: str, attributes: Dict[str, str], relay_buffer: Optional[TelemetryRelayBuffer]
) -> InstalledSDK:
    """Install the tracer, meter, and logger providers.

    They export over OTLP/HTTP, or into `relay_buffer` when one is given, for the process to send through the scheduler.
    """
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": SERVICE_NAMESPACE,
            "service.version": __version__,
            "service.instance.id": uuid.uuid4().hex,
            "host.name": socket.gethostname(),
            "process.pid": os.getpid(),
            **attributes,
        }
    )

    span_exporter: SpanExporter = OTLPSpanExporter() if relay_buffer is None else RelaySpanExporter(relay_buffer)
    metric_exporter: MetricExporter = (
        OTLPMetricExporter() if relay_buffer is None else RelayMetricExporter(relay_buffer)
    )
    log_exporter: LogRecordExporter = OTLPLogExporter() if relay_buffer is None else RelayLogExporter(relay_buffer)

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BaggageSpanProcessor())
    tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(resource=resource, metric_readers=[PeriodicExportingMetricReader(metric_exporter)])
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    set_logger_provider(logger_provider)
    return InstalledSDK(tracer_provider, meter_provider, logger_provider)


def instrument_aws_api_calls() -> None:
    """Span each boto3 API call, so a worker manager's cloud calls nest under its create_unit and destroy_unit spans."""
    if importlib.util.find_spec("botocore") is None:
        return  # the aws and orb extras install it

    from opentelemetry.instrumentation.botocore import BotocoreInstrumentor  # imports botocore

    BotocoreInstrumentor().instrument()


def attach_log_handler(logger_provider: LoggerProvider, logging_level: int) -> None:
    """Export the `scaler` logger's records; a no-op when the handler is already attached."""
    scaler_logger = logging.getLogger(SCALER_LOGGER_NAME)
    if any(isinstance(handler, LoggingHandler) for handler in scaler_logger.handlers):
        return

    scaler_logger.addHandler(LoggingHandler(level=logging_level, logger_provider=logger_provider))
