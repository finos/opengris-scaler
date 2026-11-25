import os
import unittest
from unittest import mock

import boto3
from botocore.stub import Stubber
from opentelemetry import trace
from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from scaler.utility import telemetry, telemetry_sdk
from scaler.utility.telemetry import ServiceName, TelemetryService

AWS_REGION = "us-east-1"


class TestAWSAPICallSpans(unittest.TestCase):
    def test_each_boto3_call_is_a_span_under_the_current_span(self) -> None:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))

        telemetry_sdk.instrument_aws_api_calls()
        self.addCleanup(BotocoreInstrumentor().uninstrument)

        client = boto3.client("ec2", region_name=AWS_REGION, aws_access_key_id="test", aws_secret_access_key="test")
        # the instrumentor takes its tracer from the global provider at the first call
        with (
            mock.patch.object(trace, "get_tracer_provider", return_value=provider),
            Stubber(client) as stubber,
            provider.get_tracer(__name__).start_as_current_span("create_unit") as parent,
        ):
            stubber.add_response("describe_instances", {"Reservations": []})
            client.describe_instances()

        (call,) = [span for span in exporter.get_finished_spans() if span.name == "EC2.DescribeInstances"]
        self.assertIsNotNone(call.parent)
        self.assertEqual(call.parent.span_id, parent.get_span_context().span_id)
        self.assertEqual(call.attributes["rpc.method"], "DescribeInstances")

    def test_only_a_worker_manager_instruments_its_aws_calls(self) -> None:
        for service_name, expected_calls in [(ServiceName.WORKER_MANAGER, 1), (ServiceName.WORKER, 0)]:
            with (
                mock.patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318"}),
                mock.patch.object(telemetry, "_installed_sdk", None),
                mock.patch.object(telemetry_sdk, "install_sdk"),
                mock.patch.object(telemetry_sdk, "attach_log_handler"),
                mock.patch.object(telemetry_sdk, "instrument_aws_api_calls") as instrument,
            ):
                telemetry.configure_telemetry(TelemetryService(service_name), "INFO")
            self.assertEqual(instrument.call_count, expected_calls, service_name)
