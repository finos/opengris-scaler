"""OpenTelemetry setup for Scaler processes.

Instrumentation calls the OpenTelemetry API everywhere; the API is a no-op until `configure_telemetry` installs the SDK.
"""

import dataclasses
import enum
import logging
import os
from typing import Any, Dict, Optional

from opentelemetry import propagate
from opentelemetry.context import Context

from scaler.protocol.capnp import TraceContext
from scaler.utility.logging.utility import LoggingLevel
from scaler.utility.semantic_conventions import UNIT_ID_ATTRIBUTE, WORKER_MANAGER_ID_ATTRIBUTE

logger = logging.getLogger(__name__)

OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
SDK_DISABLED_ENV = "OTEL_SDK_DISABLED"

TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"
BAGGAGE_HEADER = "baggage"

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


def worker_manager_service(worker_manager_id: str, unit_id: Optional[str]) -> TelemetryService:
    attributes = {WORKER_MANAGER_ID_ATTRIBUTE: worker_manager_id}
    if unit_id is not None:
        attributes[UNIT_ID_ATTRIBUTE] = unit_id
    return TelemetryService(ServiceName.WORKER_MANAGER, attributes)


def empty_trace_context() -> TraceContext:
    """A trace context with no span; a locally built message reads every field it sets, so all three are set."""
    return TraceContext(traceparent="", tracestate="", baggage="")


def inject_trace_context(context: Optional[Context] = None) -> TraceContext:
    """The W3C headers of `context`, or of the current context, all empty when no span is recording."""
    carrier: Dict[str, str] = {}
    propagate.inject(carrier, context=context)
    return TraceContext(
        traceparent=carrier.get(TRACEPARENT_HEADER, ""),
        tracestate=carrier.get(TRACESTATE_HEADER, ""),
        baggage=carrier.get(BAGGAGE_HEADER, ""),
    )


def extract_trace_context(trace_context: TraceContext) -> Context:
    headers = {
        TRACEPARENT_HEADER: trace_context.traceparent,
        TRACESTATE_HEADER: trace_context.tracestate,
        BAGGAGE_HEADER: trace_context.baggage,
    }
    return propagate.extract({name: value for name, value in headers.items() if value})


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
