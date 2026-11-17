"""OpenTelemetry setup for Scaler processes.

Instrumentation calls the OpenTelemetry API everywhere; the API is a no-op until `configure_telemetry` installs the SDK.
"""

import dataclasses
import enum
import logging
import os
import socket
import uuid
from typing import Any, Dict, Optional

from scaler.about import __version__
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.logging.utility import LoggingLevel

logger = logging.getLogger(__name__)

OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
SDK_DISABLED_ENV = "OTEL_SDK_DISABLED"

SERVICE_NAMESPACE = "scaler"
SCALER_LOGGER_NAME = "scaler"

EVENT_NAME_ATTRIBUTE = "event.name"
CLIENT_ID_ATTRIBUTE = "scaler.client.id"
WORKER_ID_ATTRIBUTE = "scaler.worker.id"
WORKER_MANAGER_ID_ATTRIBUTE = "scaler.worker_manager.id"
UNIT_ID_ATTRIBUTE = "scaler.unit.id"

# the SDK logger provider, set once per process; `None` while telemetry is off
_logger_provider: Optional[Any] = None


class ServiceName(enum.Enum):
    SCHEDULER = "scaler-scheduler"
    WORKER = "scaler-worker"
    PROCESSOR = "scaler-processor"
    WORKER_MANAGER = "scaler-worker-manager"


@dataclasses.dataclass(frozen=True)
class TelemetryService:
    """What a process reports itself as: the service name and the resource attributes that identify it."""

    name: ServiceName
    attributes: Dict[str, str] = dataclasses.field(default_factory=dict)


def worker_manager_service(worker_manager_id: WorkerManagerID, unit_id: Optional[str]) -> TelemetryService:
    attributes = {WORKER_MANAGER_ID_ATTRIBUTE: str(worker_manager_id)}
    if unit_id is not None:
        attributes[UNIT_ID_ATTRIBUTE] = unit_id
    return TelemetryService(ServiceName.WORKER_MANAGER, attributes)


def is_export_configured() -> bool:
    """True when this process exports telemetry directly to a collector."""
    if os.environ.get(SDK_DISABLED_ENV, "").lower() == "true":
        return False
    return bool(os.environ.get(OTLP_ENDPOINT_ENV))


def configure_telemetry(service: TelemetryService, logging_level: str) -> None:
    """Install the SDK with OTLP/HTTP exporters for traces, metrics, and the `scaler` logger, when an endpoint is set.

    Safe to call again after logging is reconfigured: the SDK is installed once, the log handler is re-attached.
    The SDK reads every other setting (sampler, headers, intervals, extra resource attributes) from `OTEL_*` variables.
    """
    global _logger_provider

    if not is_export_configured():
        return

    if _logger_provider is None:
        _logger_provider = _install_sdk(service)
        logger.info(f"exporting telemetry as {service.name.value!r} to {os.environ[OTLP_ENDPOINT_ENV]!r}")

    _attach_log_handler(_logger_provider, logging_level)


def _install_sdk(service: TelemetryService) -> Any:
    try:
        from opentelemetry import metrics, trace
        from opentelemetry._logs import set_logger_provider
        from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as e:
        raise ImportError(
            f"{OTLP_ENDPOINT_ENV} is set but the OpenTelemetry SDK is missing: install opengris-scaler[otel]"
        ) from e

    resource = Resource.create(
        {
            "service.name": service.name.value,
            "service.namespace": SERVICE_NAMESPACE,
            "service.version": __version__,
            "service.instance.id": uuid.uuid4().hex,
            "host.name": socket.gethostname(),
            "process.pid": os.getpid(),
            **service.attributes,
        }
    )

    tracer_provider = TracerProvider(resource=resource)
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


def _attach_log_handler(logger_provider: Any, logging_level: str) -> None:
    from opentelemetry.sdk._logs import LoggingHandler

    scaler_logger = logging.getLogger(SCALER_LOGGER_NAME)
    if any(isinstance(handler, LoggingHandler) for handler in scaler_logger.handlers):
        return

    scaler_logger.addHandler(LoggingHandler(level=LoggingLevel[logging_level].value, logger_provider=logger_provider))
