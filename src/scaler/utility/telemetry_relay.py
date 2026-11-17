"""The buffer between a relaying process's exporters, which run on SDK threads, and its connector to the scheduler."""

import logging
import threading
from typing import List

from scaler.io.mixins import AsyncConnector
from scaler.protocol.capnp import TelemetryBatch
from scaler.utility.mixins import Looper

logger = logging.getLogger(__name__)

# OTLP export requests a process holds while its connector is slow; the SDK batches up to 512 spans per request
RELAY_BUFFER_CAPACITY = 1024

# how often a relaying worker sends its buffered batches; the SDK's own export delays are 5s for spans and logs
RELAY_SEND_INTERVAL_SECONDS = 1


class TelemetryRelayBuffer:
    """A bounded, thread-safe queue of OTLP export requests; a full buffer drops new requests and counts them."""

    def __init__(self, capacity: int = RELAY_BUFFER_CAPACITY) -> None:
        self._capacity = capacity
        self._lock = threading.Lock()
        self._batches: List[TelemetryBatch] = []
        self._dropped = 0

    def put(self, signal: TelemetryBatch.Signal, payload: bytes) -> None:
        with self._lock:
            if len(self._batches) < self._capacity:
                self._batches.append(TelemetryBatch(signal=signal, payload=payload))
                return
            self._dropped += 1
            dropped = self._dropped

        # logged outside the lock: the log record is itself exported, and its exporter calls put
        if dropped == 1:
            logger.warning(f"telemetry relay buffer is full at {self._capacity} batches, dropping telemetry")

    def drain(self) -> List[TelemetryBatch]:
        """Take every buffered batch, and log how many were dropped since the last drain."""
        with self._lock:
            batches, self._batches = self._batches, []
            dropped, self._dropped = self._dropped, 0

        if dropped:
            logger.warning(f"telemetry relay dropped {dropped} batch(es) while its buffer was full")
        return batches


class TelemetryRelaySender(Looper):
    """Sends the batches a relaying process's exporters buffered to the scheduler, from the process's event loop."""

    def __init__(self, relay_buffer: TelemetryRelayBuffer, connector: AsyncConnector) -> None:
        self._relay_buffer = relay_buffer
        self._connector = connector

    async def routine(self) -> None:
        for batch in self._relay_buffer.drain():
            await self._connector.send(batch, detached=True)
