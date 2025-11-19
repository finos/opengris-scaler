import datetime
import hashlib
import logging
from collections import deque
from enum import Enum
from threading import Lock
from typing import Deque, Dict, List, Optional, Set, Tuple

from nicegui import ui

from scaler.protocol.python.common import TaskState, WorkerState
from scaler.protocol.python.message import StateTask, StateWorker
from scaler.ui.common.setting_page import Settings
from scaler.ui.common.utility import (
    COMPLETED_TASK_STATUSES,
    display_capabilities,
    format_timediff,
    format_worker_name,
    get_bounds,
    make_taskstream_ticks,
    make_tick_text,
)
from scaler.ui.util import NICEGUI_MAJOR_VERSION

TASK_STREAM_BACKGROUND_COLOR = "white"
TASK_STREAM_BACKGROUND_COLOR_RGB = "#000000"


class TaskShape(Enum):
    NONE = ""
    FAILED = "x"
    CANCELED = "/"


class TaskShapes:
    _task_status_to_shape = {
        TaskState.Success: TaskShape.NONE.value,
        TaskState.Running: TaskShape.NONE.value,
        TaskState.Failed: TaskShape.FAILED.value,
        TaskState.FailedWorkerDied: TaskShape.FAILED.value,
        TaskState.Canceled: TaskShape.CANCELED.value,
        TaskState.CanceledNotFound: TaskShape.CANCELED.value,
    }

    _task_shape_to_outline = {
        TaskState.Success: ("black", 1),
        TaskState.Running: ("yellow", 1),
        TaskState.Failed: ("red", 1),
        TaskState.FailedWorkerDied: ("red", 1),
        TaskState.Canceled: ("black", 1),
        TaskState.CanceledNotFound: ("black", 1),
    }

    @classmethod
    def from_status(cls, status: TaskState) -> str:
        return cls._task_status_to_shape[status]

    @classmethod
    def get_outline(cls, status: TaskState) -> Tuple[str, int]:
        return cls._task_shape_to_outline[status]


class TaskStream:
    def __init__(self):
        self._card: Optional[ui.card] = None
        self._figure = {}
        self._plot = None

        self._settings: Optional[Settings] = None

        self._start_time = datetime.datetime.now() - datetime.timedelta(minutes=30)
        self._user_axis_range: Optional[List[int]] = None

        self._current_tasks: Dict[str, Dict[bytes, datetime.datetime]] = {}
        self._completed_data_cache: Dict[str, Dict] = {}

        self._worker_to_object_name: Dict[str, str] = {}
        self._worker_to_task_ids: Dict[str, Set[bytes]] = {}
        self._worker_capabilities: Dict[str, Set[str]] = {}
        self._task_id_to_worker: Dict[bytes, str] = {}
        self._task_id_to_printable_capabilities: Dict[bytes, str] = {}

        self._seen_workers = set()

        self._data_update_lock = Lock()

        self._dead_workers: Deque[Tuple[datetime.datetime, str]] = deque()  # type: ignore[misc]

        self._capabilities_color_map: Dict[str, str] = {"<no capabilities>": "green"}

        self._worker_rows: Dict[str, List[str]] = {}
        self._row_to_worker: Dict[str, str] = {}
        self._task_row_assignment: Dict[bytes, str] = {}
        self._row_last_used: Dict[str, datetime.datetime] = {}

    def __set_worker_rows(self, worker: str, rows: List[str]):
        self._worker_rows[worker] = rows
        for row in rows:
            self._row_to_worker[row] = worker

    def __add_worker_row(self, worker: str, row: str):
        self._worker_rows[worker].append(row)
        self._row_to_worker[row] = worker

    def __clear_worker_rows(self, worker: str) -> List[str]:
        rows = self._worker_rows.pop(worker, [])
        for row in rows:
            self._row_to_worker.pop(row, None)
            self._row_last_used.pop(row, None)
        return rows

    def __make_initial_row(self, worker: str):
        base = format_worker_name(worker)
        rows = self._worker_rows.get(worker)
        if rows is None:
            # first row is [1]
            rows = [f"{base} [1]"]
            self.__set_worker_rows(worker, rows)

    def __allocate_row_for_task(self, worker: str, task_id: bytes) -> str:
        if task_id in self._task_row_assignment:
            return self._task_row_assignment[task_id]

        if worker not in self._worker_rows:
            self.__make_initial_row(worker)

        # pick first unused row label
        used_rows = set(self._task_row_assignment.values())
        for row in self._worker_rows[worker]:
            if row not in used_rows:
                self._task_row_assignment[task_id] = row

                if row not in self._completed_data_cache:
                    self.__setup_row_cache(row)
                row_data = self._completed_data_cache.get(row)
                if row_data is not None and row not in row_data.get("y", []):
                    now = datetime.datetime.now()
                    self.__add_bar(
                        worker=worker,
                        time_taken=format_timediff(self._start_time, now),
                        task_color=TASK_STREAM_BACKGROUND_COLOR,
                        state=None,
                        hovertext="",
                        capabilities_display_string="",
                        row_label=row,
                    )
                    self._row_last_used[row] = now

                return row

        # need to add a new row
        new_row = f"{format_worker_name(worker)} [{len(self._worker_rows[worker]) + 1}]"
        self.__add_worker_row(worker, new_row)
        self._task_row_assignment[task_id] = new_row

        self.__setup_row_cache(new_row)
        row_data = self._completed_data_cache.get(new_row)
        if row_data is not None and new_row not in row_data.get("y", []):
            now = datetime.datetime.now()
            self.__add_bar(
                worker=worker,
                time_taken=format_timediff(self._start_time, now),
                task_color=TASK_STREAM_BACKGROUND_COLOR,
                state=None,
                hovertext="",
                capabilities_display_string="",
                row_label=new_row,
            )
            self._row_last_used[new_row] = now

        return new_row

    def __free_row_for_task(self, task_id: bytes):
        if task_id in self._task_row_assignment:
            self._task_row_assignment.pop(task_id)

    def setup_task_stream(self, settings: Settings):
        self._card = ui.card()
        self._card.classes("w-full").style("height: 800px; overflow:auto;")

        # TODO: remove when v1 and v2 are separated
        def html_func(x: str):
            if NICEGUI_MAJOR_VERSION < 3:
                return ui.html(x)
            return ui.html(x, sanitize=False)  # type: ignore[call-arg]

        with self._card:
            html_func(
                """
                <div style="margin-bottom:8px;">
                    <b>Legend:</b>
                    <span style="display:inline-block;width:18px;height:18px;border:2px solid black;
                        vertical-align:middle;margin-right:4px;
                        background:
                            repeating-linear-gradient(135deg, transparent, transparent 4px, black 4px, black 6px),
                            repeating-linear-gradient(225deg, transparent, transparent 4px, black 4px, black 6px);
                    "></span>
                    Failed
                    <span style="display:inline-block;width:18px;height:18px;border:2px solid black;
                        vertical-align:middle;margin-left:16px;margin-right:4px;
                        background:
                            repeating-linear-gradient(-45deg, transparent, transparent 4px, black 4px, black 6px);
                    "></span>
                    Canceled
                </div>
                """
            )
            fig = {
                "data": [],
                "layout": {
                    "barmode": "stack",
                    "autosize": True,
                    "margin": {"l": 163},
                    "xaxis": {
                        "autorange": False,
                        "range": [0, 300],
                        "showgrid": False,
                        "tickmode": "array",
                        "tickvals": [0, 50, 100, 150, 200, 250, 300],
                        "ticktext": [-300, -250, -200, -150, -100, -50, 0],
                        "zeroline": False,
                    },
                    "yaxis": {
                        "autorange": True,
                        "automargin": True,
                        "rangemode": "nonnegative",
                        "showgrid": False,
                        "type": "category",
                    },
                },
            }
            self._figure = fig
            self._completed_data_cache = {}
            self._plot = ui.plotly(self._figure).classes("w-full h-full")
            self._plot.on("plotly_relayout", self._on_plotly_relayout)
            self._settings = settings

    def _on_plotly_relayout(self, e):
        x0 = e.args.get("xaxis.range[0]")
        x1 = e.args.get("xaxis.range[1]")
        if x0 is not None and x1 is not None:
            self._user_axis_range = [x0, x1]
        else:
            self._user_axis_range = None

    def __setup_row_cache(self, row_label: str):
        if row_label in self._completed_data_cache:
            return
        self._completed_data_cache[row_label] = {
            "type": "bar",
            "name": "Completed",
            "y": [],
            "x": [],
            "orientation": "h",
            "marker": {"color": [], "width": 5, "pattern": {"shape": []}, "line": {"color": [], "width": []}},
            "hovertemplate": [],
            "hovertext": [],
            "customdata": [],
            "showlegend": False,
        }

    def __get_history_fields(self, worker: str, index: int) -> Tuple[float, str, str, str, str, str, int, str]:
        row_data = self._completed_data_cache[worker]
        time_taken = row_data["x"][index]
        color = row_data["marker"]["color"][index]
        text = row_data["hovertext"][index]
        shape = row_data["marker"]["pattern"]["shape"][index]
        capabilities = row_data["customdata"][index]["capabilities"]
        row_label = row_data["y"][index]
        outline_color = row_data["marker"]["line"]["color"][index]
        outline_width = row_data["marker"]["line"]["width"][index]
        return time_taken, color, text, shape, capabilities, row_label, outline_width, outline_color

    def __remove_last_elements(self, row_label: str):
        row_data = self._completed_data_cache[row_label]
        del row_data["y"][-1]
        del row_data["x"][-1]
        del row_data["marker"]["color"][-1]
        del row_data["marker"]["pattern"]["shape"][-1]
        del row_data["marker"]["line"]["color"][-1]
        del row_data["marker"]["line"]["width"][-1]
        del row_data["hovertext"][-1]
        del row_data["hovertemplate"][-1]
        del row_data["customdata"][-1]

    def __get_capabilities_color(self, capabilities: str) -> str:
        if capabilities not in self._capabilities_color_map:
            h = hashlib.md5(capabilities.encode()).hexdigest()
            color = f"#{h[:6]}"
            if color == TASK_STREAM_BACKGROUND_COLOR_RGB:
                color = "#0000ff"
            self._capabilities_color_map[capabilities] = color
        return self._capabilities_color_map[capabilities]

    def __get_task_color(self, task_id: bytes) -> str:
        capabilities = self._task_id_to_printable_capabilities.get(task_id, "<no capabilities>")
        color = self.__get_capabilities_color(capabilities)
        return color

    def __add_task_to_chart(self, worker: str, task_id: bytes, task_state: TaskState, task_time: float) -> str:
        task_color = self.__get_task_color(task_id)
        task_hovertext = self._worker_to_object_name.get(worker, "")
        capabilities_display_string = self._task_id_to_printable_capabilities.get(task_id, "")

        row_label = self._task_row_assignment.get(task_id)

        self.__add_bar(
            worker=worker,
            time_taken=task_time,
            task_color=task_color,
            state=task_state,
            hovertext=task_hovertext,
            capabilities_display_string=capabilities_display_string,
            row_label=row_label,
        )

        self.__free_row_for_task(task_id)

        return row_label

    def __add_bar(
        self,
        worker: str,
        time_taken: float,
        task_color: str,
        state: Optional[TaskState],
        hovertext: str,
        capabilities_display_string: str,
        row_label: Optional[str] = None,
    ):
        if row_label is None:
            row_label = f"{format_worker_name(worker)} [1]"

        if state:
            shape = TaskShapes.from_status(state)
            task_outline_color, task_outline_width = TaskShapes.get_outline(state)
            state_text = state.name
        else:
            shape = TaskShape.NONE.value
            task_outline_color, task_outline_width = ("rgba(0,0,0,0)", 0)
            state_text = ""
        row_history = self._completed_data_cache[row_label]

        # if this is the same task repeated, merge the bars
        if len(row_history["y"]) > 0:
            (
                last_time_taken,
                last_color,
                last_text,
                last_shape,
                last_capabilities,
                last_row,
                last_outline_width,
                last_outline_color,
            ) = self.__get_history_fields(row_label, -1)

            if (
                last_row == row_label
                and last_color == task_color
                and last_text == hovertext
                and last_shape == shape
                and last_capabilities == capabilities_display_string
                and last_outline_width == task_outline_width
                and last_outline_color == task_outline_color
            ):
                row_history["x"][-1] += time_taken
                row_history["customdata"][-1]["task_count"] += 1
                return

            # if there's a very short gap from last task to current task, merge the bars
            if task_color != TASK_STREAM_BACKGROUND_COLOR and len(row_history["y"]) > 2:
                (
                    _,
                    penult_color,
                    penult_text,
                    penult_shape,
                    penult_capabilities,
                    penult_row,
                    penult_outline_width,
                    penult_outline_color,
                ) = self.__get_history_fields(row_label, -2)

                if (
                    last_time_taken < 0.1
                    and penult_row == row_label
                    and penult_color == task_color
                    and penult_text == hovertext
                    and penult_shape == shape
                    and penult_capabilities == capabilities_display_string
                    and penult_outline_width == task_outline_width
                    and penult_outline_color == task_outline_color
                ):
                    row_history["x"][-2] += time_taken + last_time_taken
                    row_history["customdata"][-2]["task_count"] += 1
                    self.__remove_last_elements(row_label)
                    return

        self._completed_data_cache[row_label]["y"].append(row_label)
        self._completed_data_cache[row_label]["x"].append(time_taken)
        self._completed_data_cache[row_label]["marker"]["color"].append(task_color)
        self._completed_data_cache[row_label]["marker"]["pattern"]["shape"].append(shape)
        self._completed_data_cache[row_label]["marker"]["line"]["color"].append(task_outline_color)
        self._completed_data_cache[row_label]["marker"]["line"]["width"].append(task_outline_width)
        self._completed_data_cache[row_label]["hovertext"].append(hovertext)
        self._completed_data_cache[row_label]["customdata"].append(
            {"state": state_text, "task_count": 1, "capabilities": capabilities_display_string}
        )

        if hovertext:
            self._completed_data_cache[row_label]["hovertemplate"].append(
                "%{hovertext} (%{x})<br>"
                "%{customdata.state}<br>"
                "Tasks: %{customdata.task_count}<br>"
                "Capabilities: %{customdata.capabilities}"
            )
        else:
            self._completed_data_cache[row_label]["hovertemplate"].append("")

    def __cutoff_keep_first(self, data_list: list, cutoff_index: int):
        return [data_list[0]] + data_list[cutoff_index:]

    def __remove_old_tasks_from_cache(self, row_label: str, cutoff_index: int):
        row_data = self._completed_data_cache[row_label]
        removed_time = sum([row_data["x"][i] for i in range(1, cutoff_index)])

        row_data["y"] = self.__cutoff_keep_first(row_data["y"], cutoff_index)
        row_data["x"] = self.__cutoff_keep_first(row_data["x"], cutoff_index)
        row_data["marker"]["color"] = self.__cutoff_keep_first(row_data["marker"]["color"], cutoff_index)
        row_data["marker"]["pattern"]["shape"] = self.__cutoff_keep_first(
            row_data["marker"]["pattern"]["shape"], cutoff_index
        )
        row_data["marker"]["line"]["color"] = self.__cutoff_keep_first(
            row_data["marker"]["line"]["color"], cutoff_index
        )
        row_data["marker"]["line"]["width"] = self.__cutoff_keep_first(
            row_data["marker"]["line"]["width"], cutoff_index
        )
        row_data["hovertext"] = self.__cutoff_keep_first(row_data["hovertext"], cutoff_index)
        row_data["hovertemplate"] = self.__cutoff_keep_first(row_data["hovertemplate"], cutoff_index)

        row_data["x"][0] += removed_time

    def check_row_total_time(self, row_label: str):
        row_total_time = sum(self._completed_data_cache[row_label]["x"])
        now = datetime.datetime.now()
        since_start = format_timediff(self._start_time, now)
        delta = since_start - row_total_time

        if delta > 0.1:
            # hack. need to reproduce drifting behavior and find the cause.
            logging.info(f"Adjusting row {row_label} by {delta:.2f}s to match total runtime")
            self._completed_data_cache[row_label]["x"][0] += delta

    def __handle_task_result(self, state: StateTask, now: datetime.datetime):
        worker = self._task_id_to_worker.get(state.task_id, "")
        if worker == "":
            return

        start = None
        task_map = self._current_tasks.get(worker)
        if task_map:
            start = task_map.get(state.task_id)

        if start is None:
            # we don't know when this task started, so just ignore
            return

        task_state = state.state
        task_time = format_timediff(start, now)

        row_label = self.__add_task_to_chart(worker, state.task_id, task_state, task_time)
        self.check_row_total_time(row_label)
        with self._data_update_lock:
            self.__remove_task_from_worker(worker=worker, task_id=state.task_id, now=now, force_new_time=False)
        self._row_last_used[row_label] = now

        if worker_tasks := self._worker_to_task_ids.get(worker):
            worker_tasks.discard(state.task_id)

    def __handle_new_worker(self, worker: str, now: datetime.datetime, capabilities: Set[str]):
        if worker in self._seen_workers:
            # If for some reason we receive a task for the worker before
            # the StateWorker connect message, just update capabilities.
            logging.debug(f"Handling new worker {worker} but worker is already known.")

            if self._worker_capabilities.get(worker) is None and capabilities is not None:
                self._worker_capabilities[worker] = capabilities
            return

        row_label = f"{format_worker_name(worker)} [1]"
        if row_label not in self._completed_data_cache:
            self.__setup_row_cache(row_label)
            self.__make_initial_row(worker)
            self.__add_bar(
                worker=worker,
                time_taken=format_timediff(self._start_time, now),
                task_color=TASK_STREAM_BACKGROUND_COLOR,
                state=None,
                hovertext="",
                capabilities_display_string="",
                row_label=row_label,
            )
            self._row_last_used[row_label] = now
        self._seen_workers.add(worker)
        self._worker_capabilities[worker] = capabilities

    def __remove_task_from_worker(self, worker: str, task_id: bytes, now: datetime.datetime, force_new_time: bool):
        # Remove a single task from the worker's current task mapping.
        self.__free_row_for_task(task_id)
        self._worker_to_task_ids.get(worker, set()).discard(task_id)
        task_map = self._current_tasks.get(worker)
        if not task_map:
            return
        task_map.pop(task_id, None)
        if not task_map:
            # no more tasks for this worker, remove the worker entry
            self._current_tasks.pop(worker, None)

    def __pad_inactive_time(self, worker: str, row_label: str, now: datetime.datetime):
        """If a row is re-used, it needs to be padded until current time"""
        last_update = self._row_last_used.get(row_label)
        self._row_last_used[row_label] = now
        if not last_update:
            return
        idle_time = format_timediff(last_update, now)
        self.__add_bar(
            worker=worker,
            time_taken=idle_time,
            task_color=TASK_STREAM_BACKGROUND_COLOR,
            state=None,
            hovertext="",
            capabilities_display_string="",
            row_label=row_label,
        )

    def __handle_running_task(self, state_task: StateTask, worker: str, now: datetime.datetime):
        if state_task.task_id not in self._task_id_to_printable_capabilities:
            self._task_id_to_printable_capabilities[state_task.task_id] = display_capabilities(
                set(state_task.capabilities.keys())
            )

        # if another worker was previously assigned this task, remove it
        previous_worker = self._task_id_to_worker.get(state_task.task_id)
        if previous_worker and previous_worker != worker:
            task_start_time = self._current_tasks.get(previous_worker, {}).get(state_task.task_id)
            if task_start_time:
                duration = format_timediff(task_start_time, now)
                self.__add_task_to_chart(previous_worker, state_task.task_id, TaskState.Canceled, duration)
            self.__remove_task_from_worker(
                worker=previous_worker, task_id=state_task.task_id, now=now, force_new_time=False
            )

        self._task_id_to_worker[state_task.task_id] = worker
        self._worker_to_object_name[worker] = state_task.function_name.decode()

        worker_tasks = self._worker_to_task_ids.get(worker, set())
        worker_tasks.add(state_task.task_id)
        self._worker_to_task_ids[worker] = worker_tasks

        # Per-task start times: store start time for this task only.
        task_map = self._current_tasks.get(worker)
        if task_map:
            with self._data_update_lock:
                task_map[state_task.task_id] = now
                # allocate a row for this additional task so it shows on its own row
                row_label = self.__allocate_row_for_task(worker, state_task.task_id)
                self.__pad_inactive_time(worker, row_label, now)
            return

        # first task for this worker
        with self._data_update_lock:
            self._current_tasks[worker] = {state_task.task_id: now}
            # allocate a row for the running task
            self.__allocate_row_for_task(worker, state_task.task_id)

    def handle_worker_state(self, state_worker: StateWorker):
        worker_id = state_worker.worker_id.decode()
        worker_state = state_worker.state

        now = datetime.datetime.now()

        if worker_state == WorkerState.Connected:
            self.__handle_new_worker(worker_id, now, set(state_worker.capabilities.keys()))
        elif worker_state == WorkerState.Disconnected:
            self.mark_dead_worker(worker_id)

    def handle_task_state(self, state_task: StateTask):
        """
        The scheduler sends out `state.worker` while a Task is running.
        However, as soon as the task is done, that entry is cleared.
        A Success status will thus come with an empty `state.worker`, so
        we store this mapping ourselves based on the Running statuses we see.
        """

        task_state = state_task.state
        now = datetime.datetime.now()

        if task_state in COMPLETED_TASK_STATUSES:
            self.__handle_task_result(state_task, now)
            return

        if not (worker := state_task.worker):
            return

        worker_string = worker.decode()

        if worker_string not in self._seen_workers:
            logging.warning(
                f"Unknown worker seen in handle_task_state: {worker_string}. "
                "Did this worker connect before the UI started?"
            )
            self.__handle_new_worker(worker_string, now, set())
            return

        if task_state in {TaskState.Running}:
            self.__handle_running_task(state_task, worker_string, now)

    def __clear_all_task_data(self, task_id: bytes):
        self._task_id_to_worker.pop(task_id, None)
        self._task_id_to_printable_capabilities.pop(task_id, None)

    def __clear_all_worker_data(self, worker: str):
        self._worker_to_object_name.pop(worker, None)
        self._worker_to_task_ids.pop(worker, None)
        self._worker_capabilities.pop(worker, None)
        self.__clear_worker_rows(worker)

    def __remove_worker_from_history(self, worker: str):
        logging.info(f"Removing worker {worker} from task stream history")
        for row_label in self.__clear_worker_rows(worker):
            if row_label in self._completed_data_cache:
                self._completed_data_cache.pop(row_label)

        worker_tasks = self._worker_to_task_ids.pop(worker, set())
        for task_id in worker_tasks:
            self.__clear_all_task_data(task_id)
            self.__free_row_for_task(task_id)

        self.__clear_all_worker_data(worker)
        self._seen_workers.discard(worker)

    def __remove_old_tasks_from_history(self, store_duration: datetime.timedelta):
        for row_label in self._completed_data_cache.keys():
            row_data = self._completed_data_cache[row_label]

            storage_cutoff_index = len(row_data["x"]) - 1
            time_taken = 0
            store_seconds = store_duration.total_seconds()
            while storage_cutoff_index > 0 and time_taken < store_seconds:
                time_taken += row_data["x"][storage_cutoff_index]
                storage_cutoff_index -= 1
            if storage_cutoff_index > 0:
                self.__remove_old_tasks_from_cache(row_label, storage_cutoff_index)

    def __remove_dead_workers(self, remove_up_to: datetime.datetime):
        while self._dead_workers and self._dead_workers[0][0] < remove_up_to:
            _, worker = self._dead_workers.popleft()
            self.__remove_worker_from_history(worker)

    def __split_workers_by_status(self, now: datetime.datetime) -> List[Tuple[str, float, bytes, str]]:
        workers_doing_jobs: List[Tuple[str, float, bytes, str]] = []
        for worker, task_map in self._current_tasks.items():
            if not task_map:
                continue
            object_name = self._worker_to_object_name.get(worker, "")

            # For each concurrent task, allocate/lookup a row and produce a separate entry.
            for task_id, start_time in list(task_map.items()):
                # determine duration relative to the task's start time
                duration = format_timediff(start_time, now)
                row_label = self._task_row_assignment.get(task_id)
                if row_label is None:
                    row_label = self.__allocate_row_for_task(worker, task_id)
                    logging.warning(f"split worker needed to allocate row {row_label} for task {task_id.hex()}")

                workers_doing_jobs.append((row_label, duration, task_id, object_name))
        return workers_doing_jobs

    def mark_dead_worker(self, worker_name: str):
        now = datetime.datetime.now()
        with self._data_update_lock:
            self._current_tasks.pop(worker_name, None)
            self._dead_workers.append((now, worker_name))

    def update_plot(self):
        with self._data_update_lock:
            now = datetime.datetime.now()

            workers_doing_tasks = self.__split_workers_by_status(now)

            self.__remove_old_tasks_from_history(self._settings.memory_store_time)

            worker_retention_time = now - self._settings.memory_store_time
            self.__remove_dead_workers(worker_retention_time)

            worker_display_time = now - self._settings.stream_window
            hidden_rows = {
                row
                for row in self._row_last_used.keys()
                if self._row_last_used[row] < worker_display_time and self._row_to_worker[row] in self._dead_workers
            }

            completed_cache_values = sorted(
                list(x for x in self._completed_data_cache.values() if x["y"] and x["y"][0] not in hidden_rows),
                key=lambda d: d["y"][0],
            )

        task_ids = [t for (_, _, t, _) in workers_doing_tasks]
        task_capabilities = [
            (
                f"Capabilities: {self._task_id_to_printable_capabilities.get(task_id, '')}"
                if task_id in self._task_id_to_printable_capabilities
                else ""
            )
            for task_id in task_ids
        ]
        task_colors = [self.__get_task_color(t) for t in task_ids]
        running_shape = TaskShapes.from_status(TaskState.Running)
        working_data = {
            "type": "bar",
            "name": "Working",
            "y": [w for (w, _, _, _) in workers_doing_tasks],
            "x": [t for (_, t, _, _) in workers_doing_tasks],
            "orientation": "h",
            "text": [f for (_, _, _, f) in workers_doing_tasks],
            "hovertemplate": "%{text} (%{x})<br>%{customdata}",
            "marker": {
                "color": task_colors,
                "width": 5,
                "pattern": {"shape": [running_shape for _ in workers_doing_tasks]},
                "line": {"color": ["yellow" for _ in workers_doing_tasks], "width": [1 for _ in workers_doing_tasks]},
            },
            "textfont": {"color": "black", "outline": "white", "outlinewidth": 5},
            "customdata": task_capabilities,
            "showlegend": False,
        }
        plot_data = completed_cache_values + [working_data]
        self._figure["data"] = plot_data
        self.__render_plot(now)

    def __build_capability_boxes(self, labels: List[str], lower_bound: int, upper_bound: int) -> Tuple[Dict, Dict]:
        """
        Creates a legend-style set of colored boxes indicating worker capabilities.
        Returns two traces: one for the colored boxes, and one for white background boxes to hide bars underneath.
        """
        window = max(1, upper_bound - lower_bound)
        base_x = lower_bound + window * 0.01
        offset = window * 0.01

        worker_data: Dict[str, Dict] = {}
        for worker, capabilities in self._worker_capabilities.items():
            worker_data[worker] = {"x_vals": [], "colors": [], "customdata": []}
            # capability_count = len(capabilities)
            for index, capability in enumerate(sorted(capabilities)):
                color = self.__get_capabilities_color(capability)
                x = base_x + index * offset
                worker_data[worker]["x_vals"].append(x)
                worker_data[worker]["colors"].append(color)
                worker_data[worker]["customdata"].append(capability)

        x_vals = []
        y_vals = []
        colors = []
        customdata = []

        for row in labels:
            worker = self._row_to_worker.get(row)
            box_data = worker_data.get(worker)
            if not box_data:
                continue

            x_vals.extend(box_data["x_vals"])
            y_vals.extend([row] * len(box_data["x_vals"]))
            colors.extend(box_data["colors"])
            customdata.extend(box_data["customdata"])

        # Background trace: slightly larger white squares to hide underlying bars where capability boxes sit
        background_trace = {
            "type": "scatter",
            "mode": "markers",
            "name": "",
            "x": x_vals,
            "y": y_vals,
            "marker": {"symbol": "square", "size": 12, "color": "white", "line": {"color": "white", "width": 1}},
            "hoverinfo": "skip",
            "showlegend": False,
        }

        trace = {
            "type": "scatter",
            "mode": "markers",
            "name": "Capabilities",
            "x": x_vals,
            "y": y_vals,
            "marker": {"symbol": "square", "size": 10, "color": colors, "line": {"color": "black", "width": 1}},
            "customdata": customdata,
            "hovertemplate": "%{customdata}",
            "showlegend": False,
        }

        return trace, background_trace

    def __adjust_card_height(self, num_rows: int):
        # these are estimates, may need adjustments for large row counts
        min_height = 800
        row_height = 20
        header_height = 140
        estimated_height = header_height + (num_rows * row_height)
        target_height = max(min_height, estimated_height)
        self._card.style(f"height: {target_height}px; overflow:auto;")

    def __render_plot(self, now: datetime.datetime):
        lower_bound, upper_bound = get_bounds(now, self._start_time, self._settings)

        ticks = make_taskstream_ticks(lower_bound, upper_bound)
        tick_text = make_tick_text(int(self._settings.stream_window.total_seconds()))

        if self._user_axis_range:
            self._figure["layout"]["xaxis"]["range"] = self._user_axis_range
        else:
            self._figure["layout"]["xaxis"]["range"] = [lower_bound, upper_bound]
        self._figure["layout"]["xaxis"]["tickvals"] = ticks
        self._figure["layout"]["xaxis"]["ticktext"] = tick_text

        seen = set()
        category_array: List[str] = []
        for trace in self._figure.get("data", []):
            if trace.get("orientation") == "h" and trace.get("y"):
                for yval in trace.get("y", []):
                    if yval not in seen:
                        seen.add(yval)
                        category_array.append(yval)

        capability_traces, background_trace = self.__build_capability_boxes(category_array, lower_bound, upper_bound)

        self._figure["data"].append(background_trace)
        self._figure["data"].append(capability_traces)

        self.__adjust_card_height(len(category_array))
        self._plot.update()
