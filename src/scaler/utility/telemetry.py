"""OpenTelemetry setup for Scaler processes.

Instrumentation calls the OpenTelemetry API everywhere; the API is a no-op until `configure_telemetry` installs the SDK.
"""

import dataclasses
import enum
import logging
import os
from typing import Any, Dict, Optional

from opentelemetry import propagate, trace
from opentelemetry.context import Context

from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.logging.utility import LoggingLevel
from scaler.utility.semantic_conventions import UNIT_ID_ATTRIBUTE, WORKER_MANAGER_ID_ATTRIBUTE

logger = logging.getLogger(__name__)

OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
SDK_DISABLED_ENV = "OTEL_SDK_DISABLED"

TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"
BAGGAGE_HEADER = "baggage"

# Task.traceContext: the W3C headers in this order, joined by a newline, which no header value contains
TRACE_CONTEXT_HEADERS = (TRACEPARENT_HEADER, TRACESTATE_HEADER, BAGGAGE_HEADER)
TRACE_CONTEXT_SEPARATOR = "\n"

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


def empty_trace_context() -> str:
    """A trace context with no span or baggage, as tests set it on a locally built message."""
    return ""


def is_tracing() -> bool:
    """True when a tracer provider records spans in this process: Scaler's SDK, or one the user installed."""
    return not isinstance(trace.get_tracer_provider(), trace.ProxyTracerProvider)


def inject_trace_context(context: Optional[Context] = None) -> str:
    """The W3C headers of `context`, or of the current context, in one string; empty when there is nothing to carry."""
    carrier: Dict[str, str] = {}
    propagate.inject(carrier, context=context)
    if not carrier:
        return ""
    return TRACE_CONTEXT_SEPARATOR.join(carrier.get(header, "") for header in TRACE_CONTEXT_HEADERS)


def trace_context_field(context: Optional[Context] = None) -> Dict[str, str]:
    """The `traceContext` field of an outgoing message: none when there is nothing to carry, which costs nothing."""
    trace_context = inject_trace_context(context)
    return {"traceContext": trace_context} if trace_context else {}


def extract_trace_context(trace_context: str) -> Context:
    if not trace_context:
        return Context()
    values = trace_context.split(TRACE_CONTEXT_SEPARATOR)
    return propagate.extract({header: value for header, value in zip(TRACE_CONTEXT_HEADERS, values) if value})


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

    try:
        from scaler.utility import telemetry_sdk
    except ImportError as e:
        raise ImportError(
            f"{OTLP_ENDPOINT_ENV} is set but the OpenTelemetry SDK is missing: install opengris-scaler[otel]"
        ) from e

    if _logger_provider is None:
        _logger_provider = telemetry_sdk.install_sdk(service.name.value, service.attributes)
        logger.info(f"exporting telemetry as {service.name.value!r} to {os.environ[OTLP_ENDPOINT_ENV]!r}")

    telemetry_sdk.attach_log_handler(_logger_provider, LoggingLevel[logging_level].value)
