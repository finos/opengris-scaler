"""OpenTelemetry setup for Scaler processes.

Instrumentation calls the OpenTelemetry API everywhere; the API is a no-op until `configure_telemetry` installs the SDK.
"""

import dataclasses
import enum
import logging
import os
from typing import Any, Awaitable, Dict, List, Optional

from opentelemetry import propagate, trace
from opentelemetry.context import Context

from scaler.io.mixins import AsyncConnector
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.logging.utility import LoggingLevel
from scaler.utility.semantic_conventions import UNIT_ID_ATTRIBUTE, WORKER_MANAGER_ID_ATTRIBUTE
from scaler.utility.telemetry_relay import RELAY_SEND_INTERVAL_SECONDS, TelemetryRelayBuffer, TelemetryRelaySender

logger = logging.getLogger(__name__)

OTLP_ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
SDK_DISABLED_ENV = "OTEL_SDK_DISABLED"

TRACEPARENT_HEADER = "traceparent"
TRACESTATE_HEADER = "tracestate"
BAGGAGE_HEADER = "baggage"

# Task.traceContext: the W3C headers in this order, joined by a newline, which no header value contains
TRACE_CONTEXT_HEADERS = (TRACEPARENT_HEADER, TRACESTATE_HEADER, BAGGAGE_HEADER)
TRACE_CONTEXT_SEPARATOR = "\n"

OTEL_ENVIRONMENT_PREFIX = "OTEL_"

# OpenTelemetry's environment variable carrier: a process started inside a trace finds its parent here
HEADER_TO_ENVIRONMENT = {TRACEPARENT_HEADER: "TRACEPARENT", TRACESTATE_HEADER: "TRACESTATE", BAGGAGE_HEADER: "BAGGAGE"}

# the installed SDK (telemetry_sdk.InstalledSDK), set once per process; `None` while telemetry is off
_installed_sdk: Optional[Any] = None

# where a relaying process's exporters put their requests, for the process to send to the scheduler
_relay_buffer: Optional[TelemetryRelayBuffer] = None


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
    relay: bool = False  # send telemetry through the scheduler instead of to OTEL_EXPORTER_OTLP_ENDPOINT


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


def otel_environment() -> Dict[str, str]:
    """This process's OpenTelemetry settings, for a process it starts elsewhere to export the same way."""
    return {name: value for name, value in os.environ.items() if name.startswith(OTEL_ENVIRONMENT_PREFIX)}


def trace_context_environment() -> Dict[str, str]:
    """The current trace context as environment variables, for a process started elsewhere to join the trace."""
    carrier: Dict[str, str] = {}
    propagate.inject(carrier)
    return {
        HEADER_TO_ENVIRONMENT[header]: value for header, value in carrier.items() if header in HEADER_TO_ENVIRONMENT
    }


def process_start_context() -> Context:
    """The trace context this process was started inside, from its environment; empty when it was not."""
    carrier = {header: os.environ[name] for header, name in HEADER_TO_ENVIRONMENT.items() if os.environ.get(name)}
    return propagate.extract(carrier)


def is_export_configured() -> bool:
    """True when this process exports telemetry directly to a collector."""
    if os.environ.get(SDK_DISABLED_ENV, "").lower() == "true":
        return False
    return bool(os.environ.get(OTLP_ENDPOINT_ENV))


def configure_telemetry(service: TelemetryService, logging_level: str) -> None:
    """Install the SDK for traces, metrics, and the `scaler` logger, when the process exports or relays.

    A relaying process exports into a buffer it sends to the scheduler; any other process exports over OTLP/HTTP
    when an endpoint is set. Safe to call again after logging is reconfigured: the SDK is installed once, the log
    handler is re-attached. The SDK reads every other setting (sampler, headers, intervals) from `OTEL_*` variables.
    """
    global _installed_sdk, _relay_buffer

    if not service.relay and not is_export_configured():
        return

    try:
        from scaler.utility import telemetry_sdk
    except ImportError as e:
        raise ImportError(
            "telemetry is configured but the OpenTelemetry SDK is missing: install opengris-scaler[otel]"
        ) from e

    if _installed_sdk is None:
        _relay_buffer = TelemetryRelayBuffer() if service.relay else None
        _installed_sdk = telemetry_sdk.install_sdk(service.name.value, service.attributes, _relay_buffer)
        destination = "the scheduler" if service.relay else repr(os.environ[OTLP_ENDPOINT_ENV])
        logger.info(f"exporting telemetry as {service.name.value!r} to {destination}")

    telemetry_sdk.attach_log_handler(_installed_sdk.logger_provider, LoggingLevel[logging_level].value)


def relay_buffer() -> Optional[TelemetryRelayBuffer]:
    """The buffer this process relays through the scheduler, `None` when it does not relay."""
    return _relay_buffer


def relay_routines(connector: AsyncConnector) -> List[Awaitable[None]]:
    """The loop that sends this process's buffered telemetry over `connector`, none when the process does not relay."""
    if _relay_buffer is None:
        return []
    sender = TelemetryRelaySender(_relay_buffer, connector)
    return [create_async_loop_routine(sender.routine, RELAY_SEND_INTERVAL_SECONDS)]


async def send_remaining_telemetry(connector: Optional[AsyncConnector]) -> None:
    """Flush and send what a relaying process still buffers, before its connector closes on exit."""
    if _relay_buffer is None or connector is None:
        return
    flush_telemetry()
    for batch in _relay_buffer.drain():
        await connector.send(batch, detached=True)


def flush_telemetry() -> None:
    """Hand every finished span, metric, and log record to the exporters, before a process may be killed."""
    if _installed_sdk is not None:
        _installed_sdk.flush()
