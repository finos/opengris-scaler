import http.server
import threading
from typing import Dict, List, Optional

from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
from opentelemetry.proto.logs.v1.logs_pb2 import LogRecord
from opentelemetry.proto.trace.v1.trace_pb2 import Span

from scaler.utility.network_util import get_available_tcp_port


def attribute_value(value: AnyValue) -> object:
    return getattr(value, value.WhichOneof("value")) if value.WhichOneof("value") else None


def attributes(key_values: List[KeyValue]) -> Dict[str, object]:
    return {key_value.key: attribute_value(key_value.value) for key_value in key_values}


class ReceivedSpan:
    def __init__(self, span: Span, service_name: str):
        self.span = span
        self.service_name = service_name
        self.name = span.name
        self.attributes = attributes(list(span.attributes))


class ReceivedLog:
    def __init__(self, record: LogRecord, service_name: str):
        self.record = record
        self.service_name = service_name
        self.body = attribute_value(record.body)
        self.attributes = attributes(list(record.attributes))


class OTLPReceiver:
    """An in-process OTLP/HTTP endpoint that keeps every span, metric name, and log record it receives."""

    def __init__(self) -> None:
        self.spans: List[ReceivedSpan] = []
        self.metric_names: List[str] = []
        self.logs: List[ReceivedLog] = []
        self._lock = threading.Lock()
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._thread: Optional[threading.Thread] = None
        self.endpoint = ""

    def start(self) -> None:
        receiver = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                body = self.rfile.read(int(self.headers["Content-Length"]))
                receiver.receive(self.path, body)
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *args: object) -> None:
                pass

        port = get_available_tcp_port()
        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        self.endpoint = f"http://127.0.0.1:{port}"

    def stop(self) -> None:
        assert self._server is not None and self._thread is not None
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def receive(self, path: str, body: bytes) -> None:
        with self._lock:
            if path == "/v1/traces":
                self.__receive_traces(body)
            elif path == "/v1/metrics":
                self.__receive_metrics(body)
            elif path == "/v1/logs":
                self.__receive_logs(body)

    def span_names(self) -> List[str]:
        with self._lock:
            return [span.name for span in self.spans]

    def spans_named(self, name: str) -> List[ReceivedSpan]:
        with self._lock:
            return [span for span in self.spans if span.name == name]

    def logs_with_event(self, event_name: str) -> List[ReceivedLog]:
        with self._lock:
            return [log for log in self.logs if log.attributes.get("event.name") == event_name]

    def __receive_traces(self, body: bytes) -> None:
        request = ExportTraceServiceRequest.FromString(body)
        for resource_spans in request.resource_spans:
            service_name = str(attributes(list(resource_spans.resource.attributes)).get("service.name"))
            for scope_spans in resource_spans.scope_spans:
                self.spans.extend(ReceivedSpan(span, service_name) for span in scope_spans.spans)

    def __receive_metrics(self, body: bytes) -> None:
        request = ExportMetricsServiceRequest.FromString(body)
        for resource_metrics in request.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                self.metric_names.extend(metric.name for metric in scope_metrics.metrics)

    def __receive_logs(self, body: bytes) -> None:
        request = ExportLogsServiceRequest.FromString(body)
        for resource_logs in request.resource_logs:
            service_name = str(attributes(list(resource_logs.resource.attributes)).get("service.name"))
            for scope_logs in resource_logs.scope_logs:
                self.logs.extend(ReceivedLog(record, service_name) for record in scope_logs.log_records)
