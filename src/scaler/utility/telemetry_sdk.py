"""The OpenTelemetry SDK setup, imported only when export is configured: it needs the `otel` extra."""

import logging
import os
import socket
import uuid
from typing import Dict, Optional

from opentelemetry import baggage, metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Span, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from scaler.about import __version__
from scaler.utility.semantic_conventions import SERVICE_NAMESPACE

SCALER_LOGGER_NAME = "scaler"


class BaggageSpanProcessor(SpanProcessor):
    """Copies the baggage of a span's parent context onto the span as attributes, so a user tag reaches every span."""

    def on_start(self, span: Span, parent_context: Optional[Context] = None) -> None:
        for key, value in baggage.get_all(parent_context).items():
            span.set_attribute(key, str(value))


def install_sdk(service_name: str, attributes: Dict[str, str]) -> LoggerProvider:
    """Install the tracer, meter, and logger providers with OTLP/HTTP exporters; returns the logger provider."""
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

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BaggageSpanProcessor())
    tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(tracer_provider)

    meter_provider = MeterProvider(
        resource=resource, metric_readers=[PeriodicExportingMetricReader(OTLPMetricExporter())]
    )
    metrics.set_meter_provider(meter_provider)

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
    set_logger_provider(logger_provider)
    return logger_provider


def attach_log_handler(logger_provider: LoggerProvider, logging_level: int) -> None:
    """Export the `scaler` logger's records; a no-op when the handler is already attached."""
    scaler_logger = logging.getLogger(SCALER_LOGGER_NAME)
    if any(isinstance(handler, LoggingHandler) for handler in scaler_logger.handlers):
        return

    scaler_logger.addHandler(LoggingHandler(level=logging_level, logger_provider=logger_provider))
