import asyncio
import time
from typing import Iterable, Optional

from opentelemetry import metrics
from opentelemetry.metrics import CallbackOptions, Observation

from scaler.protocol.capnp import StateScheduler, TaskState
from scaler.utility.mixins import Looper
from scaler.utility.telemetry import WORKER_MANAGER_ID_ATTRIBUTE

METER_NAME = "scaler.scheduler"

# how often the lag monitor wakes; its oversleep is the time other callbacks held the event loop
EVENT_LOOP_LAG_SAMPLE_SECONDS = 1


class SchedulerMetrics:
    """Exports the last StateScheduler report as OpenTelemetry gauges.

    The information controller records each report on the event loop, and the SDK reads the snapshot from its export
    thread, so the callbacks never touch live controller state.
    """

    def __init__(self) -> None:
        self._state: Optional[StateScheduler] = None

        meter = metrics.get_meter(METER_NAME)
        meter.create_observable_gauge(
            "scaler.scheduler.tasks",
            callbacks=[self.__observe_tasks],
            description="tasks per state; terminal states count every task that reached them",
        )
        meter.create_observable_gauge(
            "scaler.scheduler.workers", callbacks=[self.__observe_workers], description="connected workers"
        )
        meter.create_observable_gauge(
            "scaler.scheduler.processors", callbacks=[self.__observe_processors], description="processors per state"
        )
        meter.create_observable_gauge(
            "scaler.scheduler.clients", callbacks=[self.__observe_clients], description="connected clients"
        )
        meter.create_observable_gauge(
            "scaler.scheduler.objects", callbacks=[self.__observe_objects], description="objects the scheduler tracks"
        )
        meter.create_observable_gauge(
            "scaler.object_storage.bytes",
            unit="By",
            callbacks=[self.__observe_object_storage_bytes],
            description="bytes the object storage server holds",
        )
        meter.create_observable_gauge(
            "scaler.worker_manager.pending_workers",
            callbacks=[self.__observe_pending_workers],
            description="workers requested from a worker manager that have not connected yet",
        )

    def record(self, state: StateScheduler) -> None:
        self._state = state

    def __observe_tasks(self, options: CallbackOptions) -> Iterable[Observation]:
        if self._state is None:
            return []
        return [
            Observation(pair.count, {"state": TaskState(pair.state).name})
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
            Observation(busy, {"state": "busy"}),
            Observation(idle, {"state": "idle"}),
            Observation(suspended, {"state": "suspended"}),
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
        self._lag = metrics.get_meter(METER_NAME).create_histogram(
            "scaler.scheduler.event_loop.lag", unit="s", description="time the event loop woke a coroutine late"
        )

    async def routine(self) -> None:
        start = time.monotonic()
        await asyncio.sleep(EVENT_LOOP_LAG_SAMPLE_SECONDS)
        self._lag.record(max(0.0, time.monotonic() - start - EVENT_LOOP_LAG_SAMPLE_SECONDS))
