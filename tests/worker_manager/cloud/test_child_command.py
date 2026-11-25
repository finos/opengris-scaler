import os
import tempfile
import unittest
from typing import Optional
from unittest import mock

from opentelemetry import context, trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from scaler.config.common.cloud_children import CloudChildrenConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.types.address import AddressConfig
from scaler.worker_manager.cloud.child_command import (
    check_unit_requirements_support_telemetry,
    child_link_arguments,
    load_requirements_content,
    unit_environment,
)

CREATE_UNIT_TRACE_ID = 0x0123456789ABCDEF0123456789ABCDEF
CREATE_UNIT_SPAN_ID = 0x0123456789ABCDEF


class TestLoadRequirementsContent(unittest.TestCase):
    def test_returns_literal_string(self):
        content = load_requirements_content("opengris-scaler>=1.0\nboto3\n")
        self.assertEqual(content, "opengris-scaler>=1.0\nboto3\n")

    def test_reads_valid_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("opengris-scaler\n")
            path = f.name
        try:
            content = load_requirements_content(path)
            self.assertEqual(content, "opengris-scaler\n")
        finally:
            os.unlink(path)

    def test_returns_literal_string_when_path_does_not_exist(self):
        content = load_requirements_content("/nonexistent/requirements.txt")
        self.assertEqual(content, "/nonexistent/requirements.txt")


class TestChildLinkArguments(unittest.TestCase):
    BOUND_ADDRESS = "tcp://172.31.8.78:8518"
    ADVERTISED_ADDRESS = "tcp://34.228.81.179:8518"

    def _arguments(self, advertised_children_address: Optional[str]) -> str:
        cloud_children_config = CloudChildrenConfig(
            advertised_children_address=(
                AddressConfig.from_string(advertised_children_address) if advertised_children_address else None
            )
        )
        return child_link_arguments(AddressConfig.from_string(self.BOUND_ADDRESS), cloud_children_config, "unit-1", 4)

    def test_units_dial_advertised_children_address(self):
        """Units behind NAT dial the advertised address, not the address the manager binds."""
        arguments = self._arguments(self.ADVERTISED_ADDRESS)
        self.assertIn(f"--parent-address {self.ADVERTISED_ADDRESS} ", arguments)

    def test_units_dial_children_address_without_advertised_address(self):
        """Without an advertised address, units dial the address the manager binds."""
        arguments = self._arguments(None)
        self.assertIn(f"--parent-address {self.BOUND_ADDRESS} ", arguments)

    def test_an_advertised_address_naming_port_0_takes_the_bound_port(self):
        """Behind 1:1 NAT, an advertised address naming port 0 takes the port the manager bound."""
        arguments = self._arguments("tcp://34.228.81.179:0")
        self.assertIn("--parent-address tcp://34.228.81.179:8518 ", arguments)


class TestUnitEnvironment(unittest.TestCase):
    def test_a_unit_gets_the_managers_otel_settings_and_the_create_unit_trace(self):
        """A unit starts with every OTEL_ variable of its manager and the trace context it was started in."""
        create_unit = SpanContext(
            trace_id=CREATE_UNIT_TRACE_ID, span_id=CREATE_UNIT_SPAN_ID, is_remote=False, trace_flags=TraceFlags(1)
        )
        manager_environment = {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318", "UNRELATED": "x"}
        token = context.attach(trace.set_span_in_context(NonRecordingSpan(create_unit)))
        try:
            with mock.patch.dict(os.environ, manager_environment, clear=True):
                environment = unit_environment(WorkerConfig())
        finally:
            context.detach(token)

        self.assertEqual(environment["OTEL_EXPORTER_OTLP_ENDPOINT"], "http://collector:4318")
        self.assertNotIn("UNRELATED", environment)
        self.assertEqual(environment["TRACEPARENT"], f"00-{CREATE_UNIT_TRACE_ID:032x}-{CREATE_UNIT_SPAN_ID:016x}-01")

    def test_a_worker_endpoint_replaces_the_managers_endpoint(self):
        """Units behind NAT export to the worker endpoint, not to the address this manager uses."""
        worker_config = WorkerConfig(worker_otlp_endpoint="http://203.0.113.7:4318")
        with mock.patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:4318"}, clear=True):
            self.assertEqual(unit_environment(worker_config)["OTEL_EXPORTER_OTLP_ENDPOINT"], "http://203.0.113.7:4318")

    def test_a_unit_started_outside_a_trace_gets_no_trace_context(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertEqual(unit_environment(WorkerConfig()), {})


class TestUnitRequirementsSupportTelemetry(unittest.TestCase):
    def test_exporting_units_without_the_otel_extra_fail_at_start(self):
        """A manager whose units would export without the SDK refuses to start, rather than every unit failing."""
        with mock.patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318"}):
            with self.assertRaisesRegex(ValueError, r"opengris-scaler\[otel\]"):
                check_unit_requirements_support_telemetry("opengris-scaler[aws]==2.9.0\nnumpy\n", WorkerConfig())

    def test_relaying_units_without_the_otel_extra_fail_at_start(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                relaying = WorkerConfig(telemetry_relay=True)
                check_unit_requirements_support_telemetry("opengris-scaler==2.9.0\n", relaying)

    def test_the_otel_or_all_extra_satisfies_the_check(self):
        with mock.patch.dict(os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4318"}):
            check_unit_requirements_support_telemetry("opengris-scaler[aws, otel]>=2.9\n", WorkerConfig())
            check_unit_requirements_support_telemetry(
                "opengris_scaler[all] @ git+https://github.com/x/y.git\n", WorkerConfig(telemetry_relay=True)
            )

    def test_a_worker_endpoint_alone_makes_units_export(self):
        worker_config = WorkerConfig(worker_otlp_endpoint="http://203.0.113.7:4318")
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(ValueError):
                check_unit_requirements_support_telemetry("opengris-scaler==2.9.0\n", worker_config)

    def test_units_that_do_not_export_need_no_extra(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            check_unit_requirements_support_telemetry("opengris-scaler==2.9.0\n", WorkerConfig())


class TestWorkerTelemetryDestination(unittest.TestCase):
    def test_relaying_and_a_worker_endpoint_together_are_refused(self):
        """Both say where workers send telemetry, so setting both is an error rather than one silently winning."""
        with self.assertRaisesRegex(ValueError, "mutually exclusive"):
            WorkerConfig(telemetry_relay=True, worker_otlp_endpoint="http://203.0.113.7:4318")
