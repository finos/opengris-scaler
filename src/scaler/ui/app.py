import asyncio
import datetime
import hashlib
import json
import logging
import queue
import struct
import threading
from collections import deque
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set, Tuple

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from scaler.config.section.webui import WebUIConfig
from scaler.io.sync_subscriber import ZMQSyncSubscriber
from scaler.protocol.python.common import TaskState, WorkerState
from scaler.protocol.python.message import StateBalanceAdvice, StateScheduler, StateTask, StateWorker
from scaler.protocol.python.mixins import Message
from scaler.utility.formatter import format_bytes, format_microseconds, format_percentage, format_seconds
from scaler.utility.identifiers import WorkerID
from scaler.utility.metadata.profile_result import ProfileResult

_logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

BATCH_INTERVAL_SECONDS = 0.1
TASK_LOG_MAX_SIZE = 100

COMPLETED_TASK_STATUSES = {
    TaskState.Success,
    TaskState.Canceled,
    TaskState.CanceledNotFound,
    TaskState.Failed,
    TaskState.FailedWorkerDied,
}

SLIDING_WINDOW_OPTIONS = {
    5: datetime.timedelta(minutes=5),
    10: datetime.timedelta(minutes=10),
    30: datetime.timedelta(minutes=30),
}


def _format_worker_name(worker_name: str, cutoff: int = 15) -> str:
    if len(worker_name) <= cutoff:
        return worker_name
    return worker_name[:cutoff] + "+"


def _capabilities_color(capabilities_str: str, color_map: Dict[str, str]) -> str:
    if capabilities_str not in color_map:
        h = hashlib.md5(capabilities_str.encode()).hexdigest()
        hue = int(h[:4], 16) % 360
        sat = 55 + (int(h[4:6], 16) % 20)  # 55-75%
        lit = 45 + (int(h[6:8], 16) % 15)  # 45-60%
        color_map[capabilities_str] = f"hsl({hue},{sat}%,{lit}%)"
    return color_map[capabilities_str]


def _display_capabilities(capabilities: Set[str]) -> str:
    if not capabilities:
        return "<no capabilities>"
    return " ".join(sorted(capabilities))


class TaskStreamState:
    """Server-side state for the task stream chart."""

    def __init__(self) -> None:
        self._stream_window = datetime.timedelta(minutes=5)
        self._memory_store_time = datetime.timedelta(minutes=30)

        # worker tracking
        self._seen_workers: Set[str] = set()
        self._worker_capabilities: Dict[str, Set[str]] = {}
        self._capabilities_color_map: Dict[str, str] = {"<no capabilities>": "#60a5fa"}

        # task tracking  (worker -> {task_id -> start_time})
        self._current_tasks: Dict[str, Dict[bytes, datetime.datetime]] = {}
        self._task_id_to_worker: Dict[bytes, str] = {}
        self._task_id_to_capabilities: Dict[bytes, str] = {}
        self._task_id_to_function: Dict[bytes, str] = {}
        self._worker_to_task_ids: Dict[str, Set[bytes]] = {}

        # completed bar history: worker -> list of bar dicts
        # each bar has absolute "start" and "end" timestamps
        self._bar_history: Dict[str, List[Dict[str, Any]]] = {}

        self._dead_workers: Deque[Tuple[datetime.datetime, str]] = deque()

        self._lock = threading.Lock()

    def set_stream_window(self, minutes: int) -> None:
        self._stream_window = SLIDING_WINDOW_OPTIONS.get(minutes, datetime.timedelta(minutes=5))

    def _caps_to_colors(self, caps_str: str) -> List[str]:
        """Return a list of colors for the capabilities string.

        Single-capability and no-capability tasks return one color.
        Multi-capability tasks return one color per individual capability (sorted).
        """
        if caps_str == "<no capabilities>":
            return ["#60a5fa"]
        parts = caps_str.split()
        if len(parts) <= 1:
            return [_capabilities_color(caps_str, self._capabilities_color_map)]
        return [_capabilities_color(p, self._capabilities_color_map) for p in parts]

    def _ensure_worker(self, worker: str, now: datetime.datetime) -> None:
        if worker not in self._seen_workers:
            self._seen_workers.add(worker)
            self._bar_history.setdefault(worker, [])

    def handle_worker_state(self, state_worker: StateWorker) -> None:
        worker_id = state_worker.worker_id.decode()
        worker_state = state_worker.state
        now = datetime.datetime.now()

        with self._lock:
            if worker_state == WorkerState.Connected:
                self._ensure_worker(worker_id, now)
                self._worker_capabilities[worker_id] = set(state_worker.capabilities.keys())
            elif worker_state == WorkerState.Disconnected:
                self._current_tasks.pop(worker_id, None)
                self._dead_workers.append((now, worker_id))

    def handle_task_state(self, state_task: StateTask) -> None:
        task_state = state_task.state
        now = datetime.datetime.now()

        with self._lock:
            if task_state in COMPLETED_TASK_STATUSES:
                self._handle_task_result(state_task, now)
                return

            worker = state_task.worker
            if not worker:
                return

            worker_str = worker.decode()
            self._ensure_worker(worker_str, now)
            if worker_str not in self._worker_capabilities:
                self._worker_capabilities[worker_str] = set()

            if task_state == TaskState.Running:
                self._handle_running_task(state_task, worker_str, now)

    def _handle_running_task(self, state_task: StateTask, worker: str, now: datetime.datetime) -> None:
        task_id = state_task.task_id
        caps = _display_capabilities(set(state_task.capabilities.keys()))
        self._task_id_to_capabilities[task_id] = caps
        func_name = state_task.function_name.decode()
        if func_name:
            self._task_id_to_function[task_id] = func_name

        # if reassigned from another worker, cancel old assignment
        prev_worker = self._task_id_to_worker.get(task_id)
        if prev_worker and prev_worker != worker:
            task_map = self._current_tasks.get(prev_worker, {})
            start_time = task_map.get(task_id)
            if start_time:
                self._add_bar(prev_worker, task_id, start_time, now, TaskState.Canceled)
            task_map.pop(task_id, None)
            self._worker_to_task_ids.get(prev_worker, set()).discard(task_id)

        self._task_id_to_worker[task_id] = worker
        self._worker_to_task_ids.setdefault(worker, set()).add(task_id)

        # only set start time if this is a new task (don't overwrite on repeated Running messages)
        task_map = self._current_tasks.setdefault(worker, {})
        if task_id not in task_map:
            task_map[task_id] = now

    def _handle_task_result(self, state: StateTask, now: datetime.datetime) -> None:
        task_id = state.task_id
        worker = self._task_id_to_worker.get(task_id, "")

        # fallback: use worker from the completion message itself (late-connect case)
        if not worker and state.worker:
            worker = state.worker.decode()
            self._ensure_worker(worker, now)

        if not worker:
            return

        # store capabilities/function from completion message if not already known
        if task_id not in self._task_id_to_capabilities and state.capabilities:
            self._task_id_to_capabilities[task_id] = _display_capabilities(set(state.capabilities.keys()))
        func_name = state.function_name.decode() if state.function_name else ""
        if func_name and task_id not in self._task_id_to_function:
            self._task_id_to_function[task_id] = func_name

        task_map = self._current_tasks.get(worker, {})

        # use ProfileResult duration for accurate start time when available
        start = now
        end = now
        try:
            if state.metadata and state.metadata != b"":
                profile = ProfileResult.deserialize(state.metadata)
                if profile.duration_s > 0:
                    start = now - datetime.timedelta(seconds=profile.duration_s)
        except struct.error:
            pass

        # fallback to Running message timestamp if no profile data
        if start == end and task_id in task_map:
            start = task_map[task_id]

        self._add_bar(worker, task_id, start, now, state.state)

        task_map.pop(task_id, None)
        if not task_map:
            self._current_tasks.pop(worker, None)
        self._worker_to_task_ids.get(worker, set()).discard(task_id)

    def _add_bar(
        self,
        worker: str,
        task_id: bytes,
        start_time: datetime.datetime,
        end_time: datetime.datetime,
        task_state: TaskState,
    ) -> None:
        caps = self._task_id_to_capabilities.get(task_id, "<no capabilities>")
        colors = self._caps_to_colors(caps)
        func = self._task_id_to_function.get(task_id, "")
        duration = (end_time - start_time).total_seconds()

        pattern = ""
        outline_color = "black"
        outline_width = 1
        if task_state in (TaskState.Failed, TaskState.FailedWorkerDied):
            pattern = "x"
            outline_color = "red"
        elif task_state in (TaskState.Canceled, TaskState.CanceledNotFound):
            pattern = "/"

        bar = {
            "start": start_time.timestamp(),
            "end": end_time.timestamp(),
            "color": colors,
            "caps": caps,
            "pattern": pattern,
            "outline_color": outline_color,
            "outline_width": outline_width,
            "hover": f"{func} ({duration:.2f}s) - {task_state.name}",
        }

        self._bar_history.setdefault(worker, []).append(bar)

    def _prune_old_data(self, now: datetime.datetime) -> None:
        cutoff = now - self._memory_store_time
        cutoff_ts = cutoff.timestamp()

        # remove old bars
        for worker in list(self._bar_history.keys()):
            bars = self._bar_history[worker]
            while bars and bars[0]["end"] < cutoff_ts:
                bars.pop(0)

        # remove dead workers past retention
        while self._dead_workers and self._dead_workers[0][0] < cutoff:
            _, worker = self._dead_workers.popleft()
            self._bar_history.pop(worker, None)
            self._worker_to_task_ids.pop(worker, None)
            self._worker_capabilities.pop(worker, None)
            self._seen_workers.discard(worker)

    def get_render_data(self) -> Dict[str, Any]:
        now = datetime.datetime.now()
        now_ts = now.timestamp()

        with self._lock:
            self._prune_old_data(now)
            window_seconds = self._stream_window.total_seconds()
            window_start_ts = now_ts - window_seconds

            # one row per worker, sorted by name — only include workers with visible activity
            row_labels: List[str] = []
            full_row_labels: List[str] = []
            worker_order: List[str] = []
            for worker in sorted(self._seen_workers):
                # check if worker has any running tasks
                has_running = bool(self._current_tasks.get(worker))
                # check if worker has any completed bars in the visible window
                has_visible_bars = False
                if not has_running:
                    for bar in self._bar_history.get(worker, []):
                        if bar["end"] >= window_start_ts:
                            has_visible_bars = True
                            break
                if has_running or has_visible_bars:
                    row_labels.append(_format_worker_name(worker))
                    full_row_labels.append(worker)
                    worker_order.append(worker)

            # Build bars list ordered so that:
            # - Running tasks are drawn first (behind everything)
            # - Completed bars are drawn newest-first, oldest-last (oldest on top)
            # JS hover iterates backwards, so last items = checked first = hoverable on top
            bars: List[Dict[str, Any]] = []

            # 1) Running tasks (drawn first / behind completed bars)
            for row_idx, worker in enumerate(worker_order):
                task_map = self._current_tasks.get(worker)
                if not task_map:
                    continue
                for task_id, start_time in task_map.items():
                    actual_duration = (now - start_time).total_seconds()
                    x_start = (start_time - now).total_seconds()
                    x_end = 0.0  # now
                    x_start = max(x_start, -window_seconds)
                    w = x_end - x_start
                    if w <= 0:
                        continue
                    caps = self._task_id_to_capabilities.get(task_id, "<no capabilities>")
                    colors = self._caps_to_colors(caps)
                    func = self._task_id_to_function.get(task_id, "")
                    bars.append(
                        {
                            "r": row_idx,
                            "x": x_start,
                            "w": w,
                            "cs": colors,
                            "p": "",
                            "oc": "#eab308",  # yellow for running
                            "ow": 2,
                            "h": f"{func} ({actual_duration:.1f}s) - Running",
                        }
                    )

            # 2) Completed bars in reverse order (newest first, oldest last = oldest drawn on top)
            for row_idx, worker in enumerate(worker_order):
                worker_bars = self._bar_history.get(worker, [])
                for bar in reversed(worker_bars):
                    if bar["end"] < window_start_ts:
                        continue  # outside visible window
                    # convert absolute timestamps to relative seconds from now
                    x_start = bar["start"] - now_ts  # negative
                    x_end = bar["end"] - now_ts  # negative or near-zero
                    # clip to window
                    x_start = max(x_start, -window_seconds)
                    w = x_end - x_start
                    if w <= 0:
                        continue
                    bars.append(
                        {
                            "r": row_idx,
                            "x": x_start,
                            "w": w,
                            "cs": bar["color"],
                            "p": bar["pattern"],
                            "oc": bar["outline_color"],
                            "ow": bar["outline_width"],
                            "h": bar["hover"],
                        }
                    )

            # capability legend: derived from tasks visible in the stream
            active_caps: Set[str] = set()
            # from running tasks
            for worker in worker_order:
                for task_id in self._current_tasks.get(worker, {}):
                    caps_str = self._task_id_to_capabilities.get(task_id, "<no capabilities>")
                    if caps_str != "<no capabilities>":
                        active_caps.update(caps_str.split())
            # from completed bars in the visible window
            for worker in worker_order:
                for bar in self._bar_history.get(worker, []):
                    if bar["end"] >= window_start_ts:
                        task_caps = bar.get("caps", "")
                        if task_caps and task_caps != "<no capabilities>":
                            active_caps.update(task_caps.split())

            legend: List[Dict[str, str]] = [{"name": "<no capabilities>", "color": "#60a5fa"}]
            legend.extend(
                {"name": cap, "color": _capabilities_color(cap, self._capabilities_color_map)}
                for cap in sorted(active_caps)
            )

            # time axis ticks
            ticks: List[Dict[str, Any]] = []
            num_ticks = 7
            for i in range(num_ticks):
                val = -window_seconds + i * (window_seconds / (num_ticks - 1))
                ticks.append({"val": round(val, 1), "label": f"{int(val)}s"})

        return {
            "rows": row_labels,
            "full_rows": full_row_labels,
            "bars": bars,
            "legend": legend,
            "ticks": ticks,
            "window": window_seconds,
        }


class MemoryChartState:
    """Server-side state for the memory usage chart."""

    def __init__(self) -> None:
        self._start_time = datetime.datetime.now()
        self._points: List[Tuple[float, int]] = []  # (timestamp, memory_bytes)
        self._memory_store_time = datetime.timedelta(minutes=30)
        self._memory_scale = "linear"
        self._lock = threading.Lock()

    def set_memory_scale(self, scale: str) -> None:
        if scale in ("log", "linear"):
            self._memory_scale = scale

    def handle_task_state(self, state_task: StateTask) -> None:
        if state_task.metadata == b"":
            return

        try:
            profile = ProfileResult.deserialize(state_task.metadata)
        except struct.error:
            return

        if profile.memory_peak == 0:
            return

        now = datetime.datetime.now()
        with self._lock:
            start_ts = now.timestamp() - profile.duration_s
            self._points.append((start_ts, profile.memory_peak))
            self._points.append((now.timestamp(), -profile.memory_peak))

    def get_render_data(self, window_seconds: float) -> Dict[str, Any]:
        now = datetime.datetime.now()
        now_ts = now.timestamp()
        cutoff_ts = now_ts - self._memory_store_time.total_seconds()

        with self._lock:
            # prune old points
            self._points = [(t, m) for t, m in self._points if t >= cutoff_ts]

            # build memory timeline within visible window
            events = sorted(self._points, key=lambda p: p[0])

        # accumulate memory usage
        running_mem = 0
        chart_points: List[Dict[str, Any]] = []
        for ts, delta in events:
            running_mem += delta
            if running_mem < 0:
                running_mem = 0
            x = ts - now_ts  # relative seconds
            if x < -window_seconds:
                continue
            chart_points.append({"x": round(x, 2), "y": max(running_mem, 0)})

        # always include current point
        if not chart_points or chart_points[-1]["x"] < -0.1:
            chart_points.append({"x": 0, "y": max(running_mem, 0)})

        # compute y-axis ticks
        max_mem = max((p["y"] for p in chart_points), default=0)
        max_mem = max(max_mem, 1024 * 1024 * 1024)  # minimum 1GB
        y_ticks = []
        for i in range(5):
            val = int(max_mem * i / 4)
            y_ticks.append({"val": val, "label": format_bytes(val)})

        return {"points": chart_points, "y_ticks": y_ticks, "scale": self._memory_scale, "window": window_seconds}


class WebUIApp:
    """Main application holding all server-side state and managing connections."""

    def __init__(self, config: WebUIConfig) -> None:
        self._config = config
        self._message_queue: queue.Queue[Message] = queue.Queue()
        self._clients: List[WebSocket] = []
        self._clients_lock = asyncio.Lock()

        # server-side state
        self._scheduler_data: Dict[str, Any] = {}
        self._workers_data: Dict[str, Dict[str, Any]] = {}
        self._worker_capabilities: Dict[str, Dict[str, int]] = {}
        self._task_log: Deque[Dict[str, Any]] = deque(maxlen=TASK_LOG_MAX_SIZE)
        self._active_tasks: Dict[str, Dict[str, Any]] = {}  # task_id_hex -> entry (running tasks)
        self._task_id_to_function: Dict[str, str] = {}
        self._task_stream = TaskStreamState()
        self._memory_chart = MemoryChartState()
        self._worker_processors: Dict[str, Dict[str, Any]] = {}
        self._worker_manager_map: Dict[str, str] = {}  # worker_name -> manager_id (persistent)
        self._worker_managers_data: Dict[str, Dict[str, Any]] = {}  # manager_id -> manager info
        self._dead_managers: Dict[str, float] = {}  # manager_id -> disconnect timestamp
        self._manager_color_map: Dict[str, str] = {}  # manager_id -> color hex
        self._monitor_address: str = str(config.monitor_address)

        self._settings = {"stream_window": 5, "memory_scale": "linear"}

        self._subscriber: Optional[ZMQSyncSubscriber] = None
        self._batch_task: Optional[asyncio.Task] = None

    def _on_zmq_message(self, message: Message) -> None:
        """Called from ZMQ subscriber thread. Just enqueue, don't process."""
        try:
            self._message_queue.put_nowait(message)
        except queue.Full:
            pass

    def start_subscriber(self) -> None:
        self._subscriber = ZMQSyncSubscriber(
            address=self._config.monitor_address, callback=self._on_zmq_message, topic=b"", timeout_seconds=-1
        )
        self._subscriber.daemon = True
        self._subscriber.start()

    async def start_batcher(self) -> None:
        self._batch_task = asyncio.create_task(self._batch_loop())

    async def stop_batcher(self) -> None:
        if self._batch_task:
            self._batch_task.cancel()
            try:
                await self._batch_task
            except asyncio.CancelledError:
                pass

    async def _batch_loop(self) -> None:
        """Drain message queue every BATCH_INTERVAL_SECONDS and broadcast."""
        while True:
            await asyncio.sleep(BATCH_INTERVAL_SECONDS)
            messages: List[Message] = []
            while True:
                try:
                    messages.append(self._message_queue.get_nowait())
                except queue.Empty:
                    break

            if not messages:
                continue

            # Process messages
            has_scheduler_update = False
            new_task_logs: List[Dict[str, Any]] = []
            worker_events: List[Dict[str, Any]] = []

            for msg in messages:
                try:
                    if isinstance(msg, StateScheduler):
                        self._process_scheduler(msg)
                        has_scheduler_update = True
                    elif isinstance(msg, StateWorker):
                        event = self._process_worker_state(msg)
                        if event:
                            worker_events.append(event)
                    elif isinstance(msg, StateTask):
                        log_entry = self._process_task_state(msg)
                        if log_entry:
                            new_task_logs.append(log_entry)
                    elif isinstance(msg, StateBalanceAdvice):
                        pass  # unused
                except Exception:
                    _logger.exception("error processing scheduler message")

            # Build broadcast payload
            payload: Dict[str, Any] = {}

            if has_scheduler_update:
                payload["scheduler"] = self._scheduler_data
                payload["workers"] = list(self._workers_data.values())

            if worker_events:
                payload["worker_events"] = worker_events

            if new_task_logs:
                payload["task_updates"] = new_task_logs

            # Always send chart data (auto-scrolling)
            stream_data = self._task_stream.get_render_data()
            self._enrich_stream_with_managers(stream_data)
            payload["task_stream"] = stream_data

            memory_data = self._memory_chart.get_render_data(stream_data["window"])
            payload["memory_chart"] = memory_data

            # Worker processors
            if has_scheduler_update:
                payload["processors"] = self._build_processors_data()
                payload["worker_managers"] = list(self._worker_managers_data.values())

            await self._broadcast(payload)

    def _process_scheduler(self, data: StateScheduler) -> None:
        self._scheduler_data = {
            "cpu": format_percentage(data.scheduler.cpu),
            "rss": format_bytes(data.scheduler.rss),
            "rss_free": format_bytes(data.rss_free),
            "monitor_address": self._monitor_address,
        }

        # Update persistent worker-to-manager mapping with latest data
        for manager_id_bytes, worker_ids in data.scaling_manager.managed_workers.items():
            manager_name = manager_id_bytes.decode() if manager_id_bytes else "unknown"
            for wid in worker_ids:
                self._worker_manager_map[bytes(wid).decode()] = manager_name

        # Update worker manager details from scaling_manager
        current_managers: Set[str] = set()
        for detail in data.scaling_manager.worker_manager_details:
            manager_id = detail["worker_manager_id"].decode() if detail["worker_manager_id"] else "unknown"
            current_managers.add(manager_id)
            worker_ids_for_manager = data.scaling_manager.managed_workers.get(detail["worker_manager_id"], [])
            self._worker_managers_data[manager_id] = {
                "manager_id": manager_id,
                "identity": detail["identity"],
                "last_seen": format_seconds(detail["last_seen_s"]),
                "max_task_concurrency": detail["max_task_concurrency"],
                "worker_count": len(worker_ids_for_manager),
                "capabilities": detail["capabilities"],
            }
        # Mark newly-disappeared managers with a disconnect timestamp instead of
        # removing immediately, so the UI keeps showing them for a grace period.
        now_ts = datetime.datetime.now().timestamp()
        newly_dead = set(self._worker_managers_data.keys()) - current_managers
        for mid in newly_dead:
            if mid not in self._dead_managers:
                self._dead_managers[mid] = now_ts
        # Re-alive managers that came back
        for mid in current_managers:
            self._dead_managers.pop(mid, None)
        # Evict managers that have been gone for more than 2 minutes
        manager_retention_seconds = 120
        evict = [mid for mid, ts in self._dead_managers.items() if now_ts - ts > manager_retention_seconds]
        for mid in evict:
            self._dead_managers.pop(mid)
            self._worker_managers_data.pop(mid, None)

        current_workers = set()
        now = datetime.datetime.now()
        for worker_data in data.worker_manager.workers:
            worker_name = worker_data.worker_id.decode()
            current_workers.add(worker_name)
            # ensure task stream knows about this worker (handles late UI connect)
            self._task_stream._ensure_worker(worker_name, now)
            total_proc_cpu = sum(p.resource.cpu for p in worker_data.processor_statuses)
            total_proc_rss = sum(p.resource.rss for p in worker_data.processor_statuses)
            total_rss = int(total_proc_rss / 1e6)
            rss_free = int(worker_data.rss_free / 1e6)

            self._workers_data[worker_name] = {
                "id": worker_name,
                "name": _format_worker_name(worker_name),
                "full_name": worker_name,
                "manager_id": self._worker_manager_map.get(worker_name, "\u2014"),
                "agt_cpu": round(worker_data.agent.cpu / 10, 1),
                "agt_rss": int(worker_data.agent.rss / 1e6),
                "proc_cpu": round(total_proc_cpu / 10, 1),
                "proc_rss": total_rss,
                "rss_free": rss_free,
                "total_rss": total_rss + rss_free,
                "free": worker_data.free,
                "sent": worker_data.sent,
                "queued": worker_data.queued,
                "suspended": worker_data.suspended,
                "lag": format_microseconds(worker_data.lag_us),
                "itl": worker_data.itl,
                "last_seen": format_seconds(worker_data.last_s),
                "capabilities": _display_capabilities(set(self._worker_capabilities.get(worker_name, {}).keys())),
            }

            # update processor details
            self._worker_processors[worker_name] = {
                "name": _format_worker_name(worker_name),
                "full_name": worker_name,
                "manager_id": self._worker_manager_map.get(worker_name, "\u2014"),
                "rss_free": rss_free,
                "processors": [],
            }
            max_rss = 0
            for ps in sorted(worker_data.processor_statuses, key=lambda x: x.pid):
                rss_val = int(ps.resource.rss / 1e6)
                if ps.resource.rss > max_rss:
                    max_rss = ps.resource.rss
                self._worker_processors[worker_name]["processors"].append(
                    {
                        "pid": ps.pid,
                        "cpu": round(ps.resource.cpu / 10, 1),
                        "rss": rss_val,
                        "max_rss": int(max_rss / 1e6),
                        "rss_max_gauge": rss_val + rss_free,
                        "initialized": bool(ps.initialized),
                        "has_task": bool(ps.has_task),
                        "suspended": bool(ps.suspended),
                    }
                )

        # remove dead workers
        dead = set(self._workers_data.keys()) - current_workers
        for w in dead:
            self._workers_data.pop(w, None)
            self._worker_processors.pop(w, None)
            self._worker_manager_map.pop(w, None)
            self._task_stream.handle_worker_state(
                StateWorker.new_msg(WorkerID(w.encode()), WorkerState.Disconnected, {})
            )

        # Aggregate summary stats from workers into each worker manager entry
        for manager_id, mgr_data in self._worker_managers_data.items():
            mgr_proc_cpu = 0.0
            mgr_proc_rss = 0
            mgr_free = 0
            mgr_sent = 0
            mgr_queued = 0
            mgr_suspended = 0
            worker_count = 0
            for w_data in self._workers_data.values():
                if w_data.get("manager_id") == manager_id:
                    worker_count += 1
                    mgr_proc_cpu += w_data.get("proc_cpu", 0)
                    mgr_proc_rss += w_data.get("proc_rss", 0)
                    mgr_free += w_data.get("free", 0)
                    mgr_sent += w_data.get("sent", 0)
                    mgr_queued += w_data.get("queued", 0)
                    mgr_suspended += w_data.get("suspended", 0)
            mgr_data["worker_count"] = worker_count
            mgr_data["total_proc_cpu"] = round(mgr_proc_cpu, 1)
            mgr_data["total_proc_rss"] = mgr_proc_rss
            mgr_data["total_free"] = mgr_free
            mgr_data["total_sent"] = mgr_sent
            mgr_data["total_queued"] = mgr_queued
            mgr_data["total_suspended"] = mgr_suspended

    def _process_worker_state(self, state_worker: StateWorker) -> Optional[Dict[str, Any]]:
        worker_id = state_worker.worker_id.decode()
        state = state_worker.state

        if state == WorkerState.Connected:
            self._worker_capabilities[worker_id] = state_worker.capabilities
        elif state == WorkerState.Disconnected:
            self._workers_data.pop(worker_id, None)
            self._worker_capabilities.pop(worker_id, None)
            self._worker_processors.pop(worker_id, None)

        self._task_stream.handle_worker_state(state_worker)

        return {"worker_id": worker_id, "state": state.name, "capabilities": list(state_worker.capabilities.keys())}

    def _process_task_state(self, state_task: StateTask) -> Optional[Dict[str, Any]]:
        task_id_hex = state_task.task_id.hex()
        func_name = state_task.function_name.decode()

        if func_name and task_id_hex not in self._task_id_to_function:
            self._task_id_to_function[task_id_hex] = func_name

        # forward to chart states
        self._task_stream.handle_task_state(state_task)
        self._memory_chart.handle_task_state(state_task)

        if not func_name:
            func_name = self._task_id_to_function.get(task_id_hex, "")

        worker_str = ""
        full_worker = ""
        if state_task.worker:
            full_worker = state_task.worker.decode()
            worker_str = _format_worker_name(full_worker)

        caps_str = _display_capabilities(set(state_task.capabilities.keys()))
        now = datetime.datetime.now()

        if state_task.state in COMPLETED_TASK_STATUSES:
            # preserve worker/time from active entry if completion message lacks them
            prev_entry = self._active_tasks.pop(task_id_hex, None)
            if not worker_str and prev_entry:
                worker_str = prev_entry.get("worker", "")
                full_worker = prev_entry.get("full_worker", "")
            submitted_time = prev_entry["time"] if prev_entry and "time" in prev_entry else now.timestamp()
            self._task_id_to_function.pop(task_id_hex, None)

            duration_str = "N/A"
            peak_mem_str = "N/A"
            if state_task.metadata != b"":
                try:
                    profile = ProfileResult.deserialize(state_task.metadata)
                    duration_str = f"{profile.duration_s:.2f}s"
                    peak_mem_str = format_bytes(profile.memory_peak) if profile.memory_peak != 0 else "0"
                    # back-compute submitted time when no prior entry exists (late-connect)
                    if not prev_entry:
                        submitted_time = now.timestamp() - profile.duration_s
                except struct.error:
                    pass

            entry = {
                "task_id": task_id_hex,
                "function": func_name,
                "worker": worker_str,
                "full_worker": full_worker,
                "time": submitted_time,
                "duration": duration_str,
                "peak_mem": peak_mem_str,
                "status": state_task.state.name,
                "capabilities": caps_str,
            }
            self._task_log.appendleft(entry)
            return entry
        else:
            # running/inactive/canceling — track as active task
            prev_entry = self._active_tasks.get(task_id_hex)
            submitted_time = prev_entry["time"] if prev_entry and "time" in prev_entry else now.timestamp()
            if not worker_str and prev_entry:
                worker_str = prev_entry.get("worker", "")
                full_worker = prev_entry.get("full_worker", "")
            # remove stale completed entry if task was re-submitted
            self._task_log = deque((e for e in self._task_log if e["task_id"] != task_id_hex), maxlen=TASK_LOG_MAX_SIZE)
            entry = {
                "task_id": task_id_hex,
                "function": func_name,
                "worker": worker_str,
                "full_worker": full_worker,
                "time": submitted_time,
                "duration": "",
                "peak_mem": "",
                "status": state_task.state.name,
                "capabilities": caps_str,
            }
            self._active_tasks[task_id_hex] = entry
            return entry

    def _enrich_stream_with_managers(self, stream_data: Dict[str, Any]) -> None:
        """Add per-row manager IDs and a manager color legend to task stream data."""
        full_rows = stream_data.get("full_rows", [])
        row_managers = [self._worker_manager_map.get(w, "") for w in full_rows]
        stream_data["row_managers"] = row_managers

        seen: Set[str] = set()
        for mid in row_managers:
            if mid:
                seen.add(mid)
        manager_legend: List[Dict[str, str]] = [
            {"name": mid, "color": _capabilities_color(mid, self._manager_color_map)} for mid in sorted(seen)
        ]
        stream_data["manager_legend"] = manager_legend

    def _build_processors_data(self) -> List[Dict[str, Any]]:
        # Group workers by manager_id and include per-manager summary stats
        managers: Dict[str, List[Dict[str, Any]]] = {}
        for wp in self._worker_processors.values():
            mid = wp.get("manager_id", "—")
            managers.setdefault(mid, []).append(wp)

        # Ensure all known worker managers appear even if they have no workers
        for mid in self._worker_managers_data:
            managers.setdefault(mid, [])

        result = []
        for manager_id, workers in sorted(managers.items()):
            total_rss = 0
            total_rss_free = 0
            total_cpu = 0.0
            total_processors = 0
            active_processors = 0
            for wp in workers:
                total_rss_free += wp["rss_free"]
                for proc in wp["processors"]:
                    total_rss += proc["rss"]
                    total_cpu += proc["cpu"]
                    total_processors += 1
                    if proc["has_task"]:
                        active_processors += 1
            result.append(
                {
                    "manager_id": manager_id,
                    "worker_count": len(workers),
                    "total_rss": total_rss,
                    "total_rss_free": total_rss_free,
                    "total_cpu": round(total_cpu, 1),
                    "total_processors": total_processors,
                    "active_processors": active_processors,
                    "workers": workers,
                }
            )
        return result

    def update_settings(self, settings: Dict[str, Any]) -> None:
        if "stream_window" in settings:
            val = int(settings["stream_window"])
            if val in SLIDING_WINDOW_OPTIONS:
                self._settings["stream_window"] = val
                self._task_stream.set_stream_window(val)
        if "memory_scale" in settings:
            scale = str(settings["memory_scale"])
            if scale in ("log", "linear"):
                self._settings["memory_scale"] = scale
                self._memory_chart.set_memory_scale(scale)

    def _drain_pending_messages(self) -> None:
        """Process any pending messages from the queue immediately.

        Called before building a full-state snapshot so a freshly connected
        browser always sees the latest data."""
        while True:
            try:
                msg = self._message_queue.get_nowait()
            except queue.Empty:
                break
            try:
                if isinstance(msg, StateScheduler):
                    self._process_scheduler(msg)
                elif isinstance(msg, StateWorker):
                    self._process_worker_state(msg)
                elif isinstance(msg, StateTask):
                    self._process_task_state(msg)
            except Exception:
                _logger.exception("error processing scheduler message during drain")

    def get_full_state(self) -> Dict[str, Any]:
        """Get complete current state for a newly connected client."""
        # Flush any messages that arrived since the last batch-loop iteration so
        # the snapshot is as fresh as possible.
        self._drain_pending_messages()

        stream_data = self._task_stream.get_render_data()
        self._enrich_stream_with_managers(stream_data)
        memory_data = self._memory_chart.get_render_data(stream_data["window"])
        # combine active + completed for initial task log, sorted by time (newest first)
        initial_task_log = list(self._active_tasks.values()) + list(self._task_log)
        initial_task_log.sort(key=lambda e: e.get("time", 0), reverse=True)
        return {
            "scheduler": self._scheduler_data,
            "workers": list(self._workers_data.values()),
            "task_log": initial_task_log,
            "task_stream": stream_data,
            "memory_chart": memory_data,
            "processors": self._build_processors_data(),
            "worker_managers": list(self._worker_managers_data.values()),
            "settings": self._settings,
        }

    async def add_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.append(ws)

    async def remove_client(self, ws: WebSocket) -> None:
        async with self._clients_lock:
            self._clients.remove(ws)

    async def _broadcast(self, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload)
        async with self._clients_lock:
            dead: List[WebSocket] = []
            for ws in self._clients:
                try:
                    await ws.send_text(data)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self._clients.remove(ws)


def create_app(config: WebUIConfig) -> FastAPI:
    app_state = WebUIApp(config)

    # Start ZMQ subscriber immediately so messages are collected even while uvicorn
    # is still initialising.  The subscriber thread puts into a thread-safe queue;
    # the asyncio batch_loop (started in the startup event) drains it later.
    app_state.start_subscriber()

    app = FastAPI(title="Scaler Web UI")

    @app.middleware("http")
    async def no_cache_headers(request: Request, call_next):  # type: ignore[no-untyped-def]
        response = await call_next(request)
        if request.url.path.startswith("/static"):
            response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        return response

    @app.on_event("startup")
    async def startup() -> None:
        await app_state.start_batcher()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await app_state.stop_batcher()
        if app_state._subscriber:
            app_state._subscriber.destroy()

    @app.get("/")
    async def index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        await app_state.add_client(ws)
        try:
            # send full state on connect
            full_state = app_state.get_full_state()
            full_state["type"] = "full_state"
            await ws.send_text(json.dumps(full_state))

            # listen for client messages (settings changes)
            while True:
                data = await ws.receive_text()
                try:
                    msg = json.loads(data)
                    if msg.get("type") == "settings":
                        app_state.update_settings(msg.get("settings", {}))
                except (json.JSONDecodeError, KeyError):
                    pass
        except WebSocketDisconnect:
            pass
        finally:
            await app_state.remove_client(ws)

    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    return app
