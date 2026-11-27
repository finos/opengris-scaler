"""OpenTelemetry setup for Scaler processes.

Instrumentation calls the OpenTelemetry API everywhere; the API is a no-op until `configure_telemetry` installs the SDK.
"""

import dataclasses
import enum
import logging
import os
from typing import Any, Awaitable, Dict, List, Optional, Tuple

from opentelemetry import propagate
from opentelemetry.context import Context

from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.constants import (
    BAGGAGE_ENV,
    BAGGAGE_HEADER,
    OTEL_ENVIRONMENT_PREFIX,
    OTLP_ENDPOINT_ENV,
    RELAY_SEND_INTERVAL_SECONDS,
    SDK_DISABLED_ENV,
    TRACEPARENT_ENV,
    TRACEPARENT_HEADER,
    TRACESTATE_ENV,
    TRACESTATE_HEADER,
)
from scaler.io.mixins import AsyncConnector
from scaler.utility.event_loop import create_async_loop_routine
from scaler.utility.logging.utility import LoggingLevel
from scaler.utility.semantic_conventions import UNIT_ID_ATTRIBUTE, WORKER_MANAGER_ID_ATTRIBUTE
from scaler.utility.telemetry_relay import TelemetryRelayBuffer, TelemetryRelaySender

logger = logging.getLogger(__name__)

# Task.traceContext: the W3C headers in this order, joined by a newline, which no header value contains
TRACE_CONTEXT_HEADERS = (TRACEPARENT_HEADER, TRACESTATE_HEADER, BAGGAGE_HEADER)
TRACE_CONTEXT_SEPARATOR = "\n"

# OpenTelemetry's environment variable carrier: a process started inside a trace finds its parent here
HEADER_TO_ENVIRONMENT = {
    TRACEPARENT_HEADER: TRACEPARENT_ENV,
    TRACESTATE_HEADER: TRACESTATE_ENV,
    BAGGAGE_HEADER: BAGGAGE_ENV,
}


# the installed SDK (telemetry_sdk.InstalledSDK), set once per process; `None` while telemetry is off
_installed_sdk: Optional[Any] = None

# where a relaying process's exporters put their requests, for the process to send to its parent
_relay_buffer: Optional[TelemetryRelayBuffer] = None

# a process with no endpoint of its own, kept until its parent says whether it forwards: see relay_to_parent()
_deferred_service: Optional[Tuple["TelemetryService", str]] = None


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
    relay: bool = False  # the parent forwards, so without an endpoint of its own the process relays to it
    # replaces OTEL_EXPORTER_OTLP_ENDPOINT for this process, and so for the processes it starts
    otlp_endpoint: Optional[str] = None


def worker_manager_service(worker_manager_config: WorkerManagerConfig, unit_id: Optional[str]) -> TelemetryService:
    attributes = {WORKER_MANAGER_ID_ATTRIBUTE: str(worker_manager_config.worker_manager_id)}
    if unit_id is not None:
        attributes[UNIT_ID_ATTRIBUTE] = unit_id
    return TelemetryService(ServiceName.WORKER_MANAGER, attributes, otlp_endpoint=worker_manager_config.otlp_endpoint)


def empty_trace_context() -> str:
    """A trace context with no span or baggage, as tests set it on a locally built message."""
    return ""


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

    An endpoint of its own wins: the process exports over OTLP/HTTP. Without one, a process whose parent forwards
    (`service.relay`) relays into a buffer it sends to its parent, and any other process waits for relay_to_parent().
    Safe to call again after logging is reconfigured: the SDK is installed once, the log handler is re-attached. The
    SDK reads every other setting (sampler, headers, intervals) from `OTEL_*` variables.
    """
    global _deferred_service

    if service.otlp_endpoint is not None:
        os.environ[OTLP_ENDPOINT_ENV] = service.otlp_endpoint

    if is_export_configured():
        _install(dataclasses.replace(service, relay=False), logging_level)
    elif service.relay:
        _install(service, logging_level)
    else:
        _deferred_service = (service, logging_level)


def relay_to_parent() -> None:
    """Relay this process's telemetry to its parent, which forwards it; a no-op for a process that already exports."""
    if _installed_sdk is not None or _deferred_service is None:
        return
    service, logging_level = _deferred_service
    _install(dataclasses.replace(service, relay=True), logging_level)


def accepts_relayed_telemetry() -> bool:
    """True when this process exports, directly or by relaying, so the processes it supervises may relay to it."""
    return _installed_sdk is not None


def _install(service: TelemetryService, logging_level: str) -> None:
    global _installed_sdk, _relay_buffer

    try:
        from scaler.utility import telemetry_sdk
    except ImportError as e:
        if service.relay:
            # relaying is the default, not something this process was told to do: carry on without telemetry
            logger.warning("not relaying telemetry: the OpenTelemetry SDK is missing, install opengris-scaler[otel]")
            return
        raise ImportError(
            "telemetry is configured but the OpenTelemetry SDK is missing: install opengris-scaler[otel]"
        ) from e

    if _installed_sdk is None:
        _relay_buffer = TelemetryRelayBuffer() if service.relay else None
        _installed_sdk = telemetry_sdk.install_sdk(service.name.value, service.attributes, _relay_buffer)
        if service.name == ServiceName.WORKER_MANAGER:
            telemetry_sdk.instrument_aws_api_calls()
        destination = "its parent" if service.relay else repr(os.environ[OTLP_ENDPOINT_ENV])
        logger.info(f"exporting telemetry as {service.name.value!r} to {destination}")

    telemetry_sdk.attach_log_handler(_installed_sdk.logger_provider, LoggingLevel[logging_level].value)


def relay_buffer() -> Optional[TelemetryRelayBuffer]:
    """The buffer this process relays to its parent through, `None` when it does not relay."""
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
