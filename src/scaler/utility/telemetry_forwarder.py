"""Forwards relayed OTLP export requests to the collector from a thread, so a slow collector never blocks the loop."""

import logging
import os
import queue
import threading
import urllib.error
import urllib.request
from typing import Dict, Optional, Tuple

from opentelemetry.util.re import parse_env_headers

from scaler.config.constants import FORWARD_QUEUE_CAPACITY, FORWARD_TIMEOUT_SECONDS, OTLP_ENDPOINT_ENV, OTLP_HEADERS_ENV
from scaler.protocol.capnp import TelemetryBatch
from scaler.utility.telemetry import is_export_configured

logger = logging.getLogger(__name__)


SIGNAL_PATHS: Dict[TelemetryBatch.Signal, str] = {
    TelemetryBatch.Signal.traces: "/v1/traces",
    TelemetryBatch.Signal.metrics: "/v1/metrics",
    TelemetryBatch.Signal.logs: "/v1/logs",
}


class TelemetryForwarder:
    """POSTs each relayed batch unparsed to the collector's OTLP/HTTP endpoint; a full queue drops and counts."""

    def __init__(self) -> None:
        self._endpoint = os.environ[OTLP_ENDPOINT_ENV].rstrip("/") if is_export_configured() else ""
        self._headers = {
            "Content-Type": "application/x-protobuf",
            **parse_env_headers(os.environ.get(OTLP_HEADERS_ENV, ""), liberal=True),
        }

        # a None entry stops the thread
        self._queue: "queue.Queue[Optional[Tuple[str, bytes]]]" = queue.Queue(maxsize=FORWARD_QUEUE_CAPACITY)
        self._thread: Optional[threading.Thread] = None

        self._dropped = 0
        self._failing = False
        self._warned_no_endpoint = False

    def forward(self, batch: TelemetryBatch) -> None:
        if not self._endpoint:
            if not self._warned_no_endpoint:
                logger.warning(f"received relayed telemetry, but {OTLP_ENDPOINT_ENV} is not set: dropping it")
                self._warned_no_endpoint = True
            return

        if self._thread is None:
            self._thread = threading.Thread(target=self.__run, name="TelemetryForwarder", daemon=True)
            self._thread.start()

        # copied here, on the event loop: the thread never reads the message the binder received
        url = self._endpoint + SIGNAL_PATHS[TelemetryBatch.Signal(batch.signal)]
        try:
            self._queue.put_nowait((url, bytes(batch.payload)))
        except queue.Full:
            self._dropped += 1
            if self._dropped == 1:
                logger.warning(f"telemetry forward queue is full at {FORWARD_QUEUE_CAPACITY} batches, dropping")

    def stop(self) -> None:
        if self._thread is None:
            return
        self._queue.put(None)
        self._thread.join(FORWARD_TIMEOUT_SECONDS)
        if self._dropped:
            logger.warning(f"dropped {self._dropped} relayed telemetry batch(es) while the forward queue was full")

    def __run(self) -> None:
        while True:
            entry = self._queue.get()
            if entry is None:
                return
            url, payload = entry
            self.__post(url, payload)

    def __post(self, url: str, payload: bytes) -> None:
        request = urllib.request.Request(url, data=payload, headers=self._headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=FORWARD_TIMEOUT_SECONDS):
                pass
        except (urllib.error.URLError, OSError) as e:
            if not self._failing:
                logger.warning(f"cannot forward relayed telemetry to {url!r}: {e}")
                self._failing = True
            return

        if self._failing:
            logger.info(f"forwarding relayed telemetry to {self._endpoint!r} again")
            self._failing = False
