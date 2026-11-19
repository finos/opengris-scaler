import asyncio
import time
from typing import Iterable, Optional

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

from scaler.config.constants import EVENT_LOOP_LAG_BUCKET_BOUNDARIES_SECONDS, EVENT_LOOP_LAG_SAMPLE_SECONDS
from scaler.protocol.capnp import StateScheduler, TaskState
from scaler.utility.mixins import Looper
from scaler.utility.semantic_conventions import (
    OBJECT_STORAGE_BYTES_METRIC,
    SCHEDULER_CLIENTS_METRIC,
    SCHEDULER_EVENT_LOOP_LAG_METRIC,
    SCHEDULER_METER,
    SCHEDULER_OBJECTS_METRIC,
    SCHEDULER_PROCESSORS_METRIC,
    SCHEDULER_TASKS_METRIC,
    SCHEDULER_WORKERS_METRIC,
    STATE_ATTRIBUTE,
    WORKER_MANAGER_ID_ATTRIBUTE,
    WORKER_MANAGER_PENDING_WORKERS_METRIC,
)


class SchedulerMetrics:
    """Exports the last StateScheduler report as OpenTelemetry gauges.

    The information controller records each report on the event loop, and the SDK reads the snapshot from its export
    thread, so the callbacks never touch live controller state.
    """

    def __init__(self) -> None:
        self._state: Optional[StateScheduler] = None

        meter = metrics.get_meter(SCHEDULER_METER)
        meter.create_observable_gauge(
            SCHEDULER_TASKS_METRIC,
            callbacks=[self.__observe_tasks],
            description="tasks per state; terminal states count every task that reached them",
        )
        meter.create_observable_gauge(
            SCHEDULER_WORKERS_METRIC, callbacks=[self.__observe_workers], description="connected workers"
        )
        meter.create_observable_gauge(
            SCHEDULER_PROCESSORS_METRIC, callbacks=[self.__observe_processors], description="processors per state"
        )
        meter.create_observable_gauge(
            SCHEDULER_CLIENTS_METRIC, callbacks=[self.__observe_clients], description="connected clients"
        )
        meter.create_observable_gauge(
            SCHEDULER_OBJECTS_METRIC, callbacks=[self.__observe_objects], description="objects the scheduler tracks"
        )
        meter.create_observable_gauge(
            OBJECT_STORAGE_BYTES_METRIC,
            unit="By",
            callbacks=[self.__observe_object_storage_bytes],
            description="bytes the object storage server holds",
        )
        meter.create_observable_gauge(
            WORKER_MANAGER_PENDING_WORKERS_METRIC,
            callbacks=[self.__observe_pending_workers],
            description="workers requested from a worker manager that have not connected yet",
        )

    def record(self, state: StateScheduler) -> None:
        self._state = state

    def __observe_tasks(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [
            Observation(pair.count, {STATE_ATTRIBUTE: TaskState(pair.state).name})
            for pair in self._state.taskManager.stateToCount
        ]

    def __observe_workers(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [Observation(len(self._state.workerManager.workers))]

    def __observe_processors(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []

        busy, idle, suspended = 0, 0, 0
        for worker in self._state.workerManager.workers:
            for processor in worker.processorStatuses:
                if processor.suspended:
                    suspended += 1
                elif processor.hasTask:
                    busy += 1
                else:
                    idle += 1
        return [
            Observation(busy, {STATE_ATTRIBUTE: "busy"}),
            Observation(idle, {STATE_ATTRIBUTE: "idle"}),
            Observation(suspended, {STATE_ATTRIBUTE: "suspended"}),
        ]

    def __observe_clients(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [Observation(len(self._state.clientManager.clients))]

    def __observe_objects(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [Observation(self._state.objectManager.numberOfObjects)]

    def __observe_object_storage_bytes(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [Observation(self._state.objectManager.storage.totalBytes)]

    def __observe_pending_workers(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [
            Observation(detail.pendingWorkers, {WORKER_MANAGER_ID_ATTRIBUTE: detail.workerManagerID.decode()})
            for detail in self._state.scalingManager.workerManagerDetails
        ]


class EventLoopLagMonitor(Looper):
    """Records how late the event loop wakes a sleeping coroutine, which is how long callbacks block the loop."""

    def __init__(self) -> None:
        self._lag = metrics.get_meter(SCHEDULER_METER).create_histogram(
            SCHEDULER_EVENT_LOOP_LAG_METRIC,
            unit="s",
            description="time the event loop woke a coroutine late",
            explicit_bucket_boundaries_advisory=EVENT_LOOP_LAG_BUCKET_BOUNDARIES_SECONDS,
        )

    async def routine(self) -> None:
        start = time.monotonic()
        await asyncio.sleep(EVENT_LOOP_LAG_SAMPLE_SECONDS)
        self._lag.record(max(0.0, time.monotonic() - start - EVENT_LOOP_LAG_SAMPLE_SECONDS))
