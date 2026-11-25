import asyncio
import multiprocessing
import os
import sys
import time
import unittest
from typing import Callable
from unittest import mock

from opentelemetry import baggage, context, trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

from scaler import Client, SchedulerClusterCombo
from scaler.cluster.combo import _run_native_worker_manager
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.types.address import AddressConfig, SocketType
from scaler.utility.identifiers import WorkerManagerID
from scaler.utility.logging.utility import setup_logger
from scaler.utility.network_util import OS_PICKED_PORT
from scaler.worker_manager.native.worker_manager import NativeWorkerManager
from tests.utility.otlp_receiver import OTLPReceiver
from tests.utility.utility import logging_test_name
from tests.worker_manager.test_nesting import UNIT_ID, _FakeParent

TELEMETRY_TIMEOUT_SECONDS = 30
POLL_SECONDS = 0.1

CLIENT_TRACE_ID = 0x0123456789ABCDEF0123456789ABCDEF
CLIENT_SPAN_ID = 0x0123456789ABCDEF

TASK_COUNT = 20

# Windows stops a worker or processor with TerminateProcess, which drops the telemetry it has not exported yet
WINDOWS_STOP_LOSES_TELEMETRY = "Windows stops workers and processors without letting them flush"

# the parent's create_unit span, as a cloud worker manager passes it to a unit in TRACEPARENT
PARENT_CREATE_UNIT_TRACE_ID = 0x00112233445566778899AABBCCDDEEFF
PARENT_CREATE_UNIT_SPAN_ID = 0x0011223344556677

# export within a fraction of a second, so a test waits on the cluster, not on the SDK's default 5s and 60s intervals
FAST_EXPORT_ENVIRONMENT = {
    "OTEL_METRIC_EXPORT_INTERVAL": "200",
    "OTEL_BSP_SCHEDULE_DELAY": "100",
    "OTEL_BLRP_SCHEDULE_DELAY": "100",
}


def wait_for(condition: Callable[[], bool], description: str) -> None:
    deadline = time.monotonic() + TELEMETRY_TIMEOUT_SECONDS
    while not condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {description}")
        time.sleep(POLL_SECONDS)


def traced_work(value: int) -> int:
    with trace.get_tracer(__name__).start_as_current_span("user.work"):
        return value + 1


def nested_submit(address: str) -> str:
    """Submit a task from a client nested in this one, and return that client's ID."""
    with Client(address=address) as nested_client:
        nested_client.submit(traced_work, 1).result()
        return nested_client.identity.decode()


def failing_work() -> None:
    raise ValueError("task failed on purpose")


def client_parent_context() -> context.Context:
    """A remote span with baggage, as a client whose caller is traced would have."""
    span_context = SpanContext(
        trace_id=CLIENT_TRACE_ID, span_id=CLIENT_SPAN_ID, is_remote=True, trace_flags=TraceFlags(TraceFlags.SAMPLED)
    )
    return baggage.set_baggage("job", "backtest-42", trace.set_span_in_context(NonRecordingSpan(span_context)))


class TestTelemetry(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.receiver = OTLPReceiver()
        self.receiver.start()
        self.addCleanup(self.receiver.stop)

        # spawned cluster processes read the environment when they start
        environment = mock.patch.dict(
            os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": self.receiver.endpoint, **FAST_EXPORT_ENVIRONMENT}
        )
        environment.start()
        self.addCleanup(environment.stop)

        self.cluster = SchedulerClusterCombo(n_workers=1)
        self.addCleanup(self.cluster.shutdown)

    def test_scheduler_exports_cluster_metrics(self) -> None:
        """The scheduler exports task, worker, and event loop metrics to the OTLP endpoint."""
        with Client(address=self.cluster.get_address()) as client:
            self.assertEqual(client.submit(pow, 2, 3).result(), 8)

        expected = {"scaler.scheduler.tasks", "scaler.scheduler.workers", "scaler.scheduler.event_loop.lag"}
        wait_for(lambda: expected <= set(self.receiver.metric_names), f"metrics {expected}")

    def test_scheduler_logs_worker_connect_as_event(self) -> None:
        """A worker connecting is a log record with the worker and worker manager IDs as attributes."""
        wait_for(lambda: bool(self.receiver.logs_with_event("scaler.worker.connected")), "worker connected event")

        event = self.receiver.logs_with_event("scaler.worker.connected")[0]
        self.assertEqual(event.service_name, "scaler-scheduler")
        self.assertTrue(str(event.attributes["scaler.worker.id"]).startswith("Worker|"))
        self.assertIn("scaler.worker_manager.id", event.attributes)

    def test_task_spans_nest_from_client_to_user_code(self) -> None:
        """A task's spans form one trace: client parent, scheduler, worker, processor, then the user's own span."""
        token = context.attach(client_parent_context())
        try:
            with Client(address=self.cluster.get_address()) as client:
                self.assertEqual(client.submit(traced_work, 1).result(), 2)
                client_id = client.identity.decode()
        finally:
            context.detach(token)

        span_names = ["scaler.scheduler.task", "scaler.worker.task", "scaler.processor.execute", "user.work"]
        wait_for(lambda: all(self.receiver.spans_named(name) for name in span_names), f"spans {span_names}")

        spans = [self.receiver.spans_named(name)[0] for name in span_names]
        for span in spans:
            self.assertEqual(int.from_bytes(span.span.trace_id, "big"), CLIENT_TRACE_ID, span.name)
            self.assertEqual(span.attributes.get("job"), "backtest-42", span.name)
            self.assertEqual(span.attributes.get("scaler.client.id"), client_id, span.name)

        self.assertEqual(int.from_bytes(spans[0].span.parent_span_id, "big"), CLIENT_SPAN_ID)
        for parent, child in zip(spans, spans[1:]):
            self.assertEqual(child.span.parent_span_id, parent.span.span_id, f"{child.name} under {parent.name}")

        self.assertEqual(spans[0].attributes["scaler.task.function"], "traced_work")
        self.assertEqual(spans[0].attributes["scaler.task.state"], "success")

    @unittest.skipIf(sys.platform == "win32", WINDOWS_STOP_LOSES_TELEMETRY)
    def test_a_nested_clients_spans_carry_its_own_id_inside_the_parent_task(self) -> None:
        """A nested client's task spans carry the nested client's ID, in the parent task's trace under its execute."""
        with Client(address=self.cluster.get_address()) as client:
            nested_client_id = client.submit(nested_submit, self.cluster.get_address()).result()
            client_id = client.identity.decode()

        wait_for(lambda: len(self.receiver.spans_named("scaler.processor.execute")) == 2, "both execute spans")
        wait_for(lambda: bool(self.receiver.spans_named("user.work")), "the nested task's user span")

        execute_spans = self.receiver.spans_named("scaler.processor.execute")
        executes = {span.attributes["scaler.client.id"]: span for span in execute_spans}
        self.assertEqual(set(executes), {client_id, nested_client_id})
        parent_execute, nested_execute = executes[client_id], executes[nested_client_id]

        (user_work,) = self.receiver.spans_named("user.work")
        self.assertEqual(user_work.attributes.get("scaler.client.id"), nested_client_id)
        self.assertEqual(user_work.span.parent_span_id, nested_execute.span.span_id)
        self.assertEqual(nested_execute.span.trace_id, parent_execute.span.trace_id)

        (nested_submit_span,) = self.receiver.spans_named("scaler.client.submit")
        self.assertEqual(nested_submit_span.span.parent_span_id, parent_execute.span.span_id)

    def test_client_tags_reach_every_span_with_block_tags_over_client_tags(self) -> None:
        """A client's tags reach every span of its tasks; a tags() block adds to and overrides them for its submits."""
        with Client(address=self.cluster.get_address(), tags={"team": "rates", "job": "backtest-42"}) as client:
            self.assertEqual(client.submit(traced_work, 1).result(), 2)
            with client.tags(job="rerun", stage="aggregate"):
                self.assertEqual(client.submit(traced_work, 2).result(), 3)
            self.assertEqual(client.submit(traced_work, 3).result(), 4)

        span_names = ["scaler.scheduler.task", "scaler.worker.task", "scaler.processor.execute", "user.work"]
        wait_for(lambda: all(len(self.receiver.spans_named(name)) == 3 for name in span_names), "3 tasks' spans")

        expected = [("rates", "backtest-42", None), ("rates", "rerun", "aggregate"), ("rates", "backtest-42", None)]
        for name in span_names:
            tags = sorted(
                (span.attributes.get("team"), span.attributes.get("job"), span.attributes.get("stage"))
                for span in self.receiver.spans_named(name)
            )
            self.assertEqual(tags, sorted(expected, key=str), name)

    def test_tag_keys_under_the_scaler_prefix_are_refused(self) -> None:
        """A tag may not spoof one of Scaler's own attributes, on the client or in a block."""
        with self.assertRaises(ValueError):
            Client(address=self.cluster.get_address(), tags={"scaler.client.id": "spoofed"})

        with Client(address=self.cluster.get_address()) as client:
            with self.assertRaises(ValueError):
                with client.tags(**{"scaler.task.id": "spoofed"}):
                    pass

    def test_queue_waits_are_spans_under_the_scheduler_and_worker_spans(self) -> None:
        """Each wait for a worker or a processor is a queued span that ends before the next stage starts."""
        with Client(address=self.cluster.get_address()) as client:
            self.assertEqual(client.submit(traced_work, 1).result(), 2)

        span_names = [
            "scaler.scheduler.task",
            "scaler.scheduler.queued",
            "scaler.worker.task",
            "scaler.worker.queued",
            "scaler.processor.execute",
        ]
        wait_for(lambda: all(self.receiver.spans_named(name) for name in span_names), f"spans {span_names}")
        scheduler_task, scheduler_queued, worker_task, worker_queued, execute = [
            self.receiver.spans_named(name)[0].span for name in span_names
        ]

        self.assertEqual(scheduler_queued.parent_span_id, scheduler_task.span_id)
        self.assertEqual(worker_queued.parent_span_id, worker_task.span_id)
        self.assertLessEqual(scheduler_queued.end_time_unix_nano, worker_task.start_time_unix_nano)
        self.assertLessEqual(worker_queued.end_time_unix_nano, execute.start_time_unix_nano)

    def test_failed_task_spans_record_the_error(self) -> None:
        """A task that raises ends its processor and scheduler spans with an error status."""
        with Client(address=self.cluster.get_address()) as client:
            with self.assertRaises(ValueError):
                client.submit(failing_work).result()

        wait_for(lambda: bool(self.receiver.spans_named("scaler.scheduler.task")), "scheduler task span")
        wait_for(lambda: bool(self.receiver.spans_named("scaler.processor.execute")), "processor execute span")

        execute = self.receiver.spans_named("scaler.processor.execute")[0]
        self.assertEqual(execute.span.status.code, execute.span.status.STATUS_CODE_ERROR)
        self.assertEqual([event.name for event in execute.span.events], ["exception"])

        scheduler_task = self.receiver.spans_named("scaler.scheduler.task")[0]
        self.assertEqual(scheduler_task.span.status.code, scheduler_task.span.status.STATUS_CODE_ERROR)
        self.assertEqual(scheduler_task.attributes["scaler.task.state"], "failed")

    def test_scale_up_is_one_trace_from_decision_to_connected_worker(self) -> None:
        """The scheduler's scaling decision parents the manager's unit create, which parents the worker's start."""
        span_names = ["scaler.scheduler.scaling_decision", "scaler.worker_manager.create_unit", "scaler.worker.start"]
        wait_for(lambda: all(self.receiver.spans_named(name) for name in span_names), f"spans {span_names}")

        decision, create_unit, start = [self.receiver.spans_named(name)[0] for name in span_names]
        self.assertEqual(decision.service_name, "scaler-scheduler")
        self.assertEqual(create_unit.service_name, "scaler-worker-manager")
        self.assertEqual(start.service_name, "scaler-worker")

        self.assertEqual(create_unit.span.trace_id, decision.span.trace_id)
        self.assertEqual(create_unit.span.parent_span_id, decision.span.span_id)
        self.assertEqual(start.span.parent_span_id, create_unit.span.span_id)
        self.assertEqual(start.span.status.code, start.span.status.STATUS_CODE_OK)
        self.assertEqual(decision.attributes["scaler.worker_manager.desired_task_concurrency"], 1)


class TestTelemetryRelay(unittest.TestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.receiver = OTLPReceiver()
        self.receiver.start()
        self.addCleanup(self.receiver.stop)

        environment = mock.patch.dict(
            os.environ, {"OTEL_EXPORTER_OTLP_ENDPOINT": self.receiver.endpoint, **FAST_EXPORT_ENVIRONMENT}
        )
        environment.start()
        self.addCleanup(environment.stop)

        self.cluster = SchedulerClusterCombo(n_workers=1, telemetry_relay=True)
        self.addCleanup(self.cluster.shutdown)

    def test_relayed_worker_spans_reach_the_collector_through_the_scheduler(self) -> None:
        """A relaying worker's and processor's spans arrive from the scheduler's forwarder, still in the task trace."""
        with Client(address=self.cluster.get_address()) as client:
            self.assertEqual(client.submit(traced_work, 1).result(), 2)

        span_names = ["scaler.scheduler.task", "scaler.worker.task", "scaler.processor.execute", "user.work"]
        wait_for(lambda: all(self.receiver.spans_named(name) for name in span_names), f"spans {span_names}")

        scheduler_task, worker_task, execute, user_work = [self.receiver.spans_named(name)[0] for name in span_names]
        self.assertNotIn("urllib", scheduler_task.user_agent)
        for relayed in (worker_task, execute, user_work):
            self.assertIn("urllib", relayed.user_agent, relayed.name)

        self.assertEqual(worker_task.span.parent_span_id, scheduler_task.span.span_id)
        self.assertEqual(user_work.span.parent_span_id, execute.span.span_id)
        self.assertEqual(execute.service_name, "scaler-processor")

    @unittest.skipIf(sys.platform == "win32", WINDOWS_STOP_LOSES_TELEMETRY)
    def test_a_relaying_worker_sends_its_buffered_spans_before_it_exits(self) -> None:
        """Worker spans still in the SDK's batch when the cluster stops reach the collector."""
        # the default 5s batch delay holds the worker spans in its SDK until the worker exits
        with mock.patch.dict(os.environ, {"OTEL_BSP_SCHEDULE_DELAY": "5000"}):
            cluster = SchedulerClusterCombo(n_workers=1, telemetry_relay=True)
        try:
            with Client(address=cluster.get_address()) as client:
                client.map(pow, [2] * TASK_COUNT, range(TASK_COUNT))
        finally:
            cluster.shutdown()

        wait_for(
            lambda: len(self.receiver.spans_named("scaler.worker.task")) >= TASK_COUNT,
            f"{TASK_COUNT} worker task spans",
        )


class TestTelemetryOfAUnit(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        setup_logger()
        logging_test_name(self)

        self.receiver = OTLPReceiver()
        self.receiver.start()
        self.addCleanup(self.receiver.stop)

    async def test_a_units_manager_joins_the_trace_of_the_create_unit_that_started_it(self) -> None:
        """A unit started with its parent's TRACEPARENT nests its create_unit, then its worker's start, under it."""
        traceparent = f"00-{PARENT_CREATE_UNIT_TRACE_ID:032x}-{PARENT_CREATE_UNIT_SPAN_ID:016x}-01"
        unit_environment = {
            "OTEL_EXPORTER_OTLP_ENDPOINT": self.receiver.endpoint,
            "TRACEPARENT": traceparent,
            **FAST_EXPORT_ENVIRONMENT,
        }

        combo = SchedulerClusterCombo(n_workers=0)
        parent = _FakeParent(AddressConfig(SocketType.tcp, "127.0.0.1", OS_PICKED_PORT))
        await parent.bind()  # the child dials the address the parent bound
        parent.task_concurrency = 1
        parent_task = asyncio.ensure_future(parent.run())
        # started like a unit's manager: bootstrap_process installs the SDK from the environment, then the manager runs
        child_manager = NativeWorkerManager(
            NativeWorkerManagerConfig(
                worker_manager_config=WorkerManagerConfig(
                    scheduler_address=AddressConfig.from_string(combo.get_address()),
                    worker_manager_id=WorkerManagerID.from_string("unit-telemetry-test"),
                    max_task_concurrency=1,
                ),
                worker_config=WorkerConfig(heartbeat_interval_seconds=1),
                parent_address=parent.address,
                unit_id=UNIT_ID,
            )
        )
        child_process = multiprocessing.get_context("spawn").Process(
            target=_run_native_worker_manager, args=(child_manager,)
        )
        try:
            with mock.patch.dict(os.environ, unit_environment):
                child_process.start()  # spawn copies the environment at start

            span_names = ["scaler.worker_manager.create_unit", "scaler.worker.start"]
            deadline = time.monotonic() + TELEMETRY_TIMEOUT_SECONDS
            while not all(self.receiver.spans_named(name) for name in span_names):
                self.assertLess(time.monotonic(), deadline, f"timed out waiting for spans {span_names}")
                await asyncio.sleep(POLL_SECONDS)
        finally:
            if child_process.is_alive():
                child_process.terminate()
                child_process.join()
            parent_task.cancel()
            parent.destroy()
            combo.shutdown()

        create_unit = self.receiver.spans_named("scaler.worker_manager.create_unit")[0].span
        start = self.receiver.spans_named("scaler.worker.start")[0].span
        self.assertEqual(int.from_bytes(create_unit.trace_id, "big"), PARENT_CREATE_UNIT_TRACE_ID)
        self.assertEqual(int.from_bytes(create_unit.parent_span_id, "big"), PARENT_CREATE_UNIT_SPAN_ID)
        self.assertEqual(start.parent_span_id, create_unit.span_id)
