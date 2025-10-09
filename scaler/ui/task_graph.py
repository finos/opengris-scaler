import datetime
from collections import deque
from enum import Enum
import hashlib
from queue import SimpleQueue
from threading import Lock
from typing import Deque, Dict, List, Optional, Set, Tuple

from nicegui import ui

from scaler.protocol.python.common import TaskState
from scaler.protocol.python.message import StateTask
from scaler.ui.live_display import WorkersSection
from scaler.ui.setting_page import Settings
from scaler.ui.utility import (
    COMPLETED_TASK_STATUSES,
    display_capabilities,
    format_timediff,
    format_worker_name,
    get_bounds,
    make_tick_text,
    make_ticks,
)

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
        TaskState.Success: ("black", 2),
        TaskState.Running: ("yellow", 2),
        TaskState.Failed: ("red", 2),
        TaskState.FailedWorkerDied: ("red", 2),
        TaskState.Canceled: ("black", 2),
        TaskState.CanceledNotFound: ("black", 2),
    }

    @classmethod
    def from_status(cls, status: TaskState) -> str:
        return cls._task_status_to_shape[status]

    @classmethod
    def get_outline(cls, status: TaskState) -> Tuple[str, int]:
        return cls._task_shape_to_outline[status]


class TaskStream:
    def __init__(self):
        self._figure = {}
        self._plot = None

        self._settings: Optional[Settings] = None

        self._start_time = datetime.datetime.now() - datetime.timedelta(minutes=30)
        self._last_task_tick = datetime.datetime.now()

        self._current_tasks: Dict[str, Tuple[bool, Set[bytes], Optional[datetime.datetime]]] = {}
        self._completed_data_cache: Dict[str, Dict] = {}

        self._worker_to_object_name: Dict[str, str] = {}
        self._worker_last_update: Dict[str, datetime.datetime] = {}
        self._task_id_to_worker: Dict[bytes, str] = {}
        self._task_id_to_printable_capabilities: Dict[bytes, str] = {}

        self._seen_workers = set()
        self._lost_workers_queue: SimpleQueue[Tuple[datetime.datetime, str]] = SimpleQueue()

        self._data_update_lock = Lock()
        self._busy_workers: Set[str] = set()
        self._busy_workers_update_time: datetime.datetime = datetime.datetime.now()

        self._dead_workers: Deque[Tuple[datetime.datetime, str]] = deque()  # type: ignore[misc]

        self._capabilities_color_map: Dict[str, str] = {"<no capabilities>": "green"}

    def setup_task_stream(self, settings: Settings):
        with ui.card().classes("w-full").style("height: 85vh"):
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
            self._settings = settings

    def __setup_worker_cache(self, worker: str):
        if worker in self._completed_data_cache:
            return
        self._completed_data_cache[worker] = {
            "type": "bar",
            "name": "History",
            "y": [],
            "x": [],
            "orientation": "h",
            "marker": {"color": [], "width": 5, "pattern": {"shape": []}, "line": {"color": [], "width": []}},
            "hovertemplate": [],
            "hovertext": [],
            "customdata": [],
            "showlegend": False,
        }

    def __get_history_fields(self, worker: str, index: int) -> Tuple[float, str, str, str]:
        worker_data = self._completed_data_cache[worker]
        time_taken = worker_data["x"][index]
        color = worker_data["marker"]["color"][index]
        text = worker_data["hovertext"][index]
        shape = worker_data["marker"]["pattern"]["shape"][-1]
        return time_taken, color, text, shape

    def __remove_last_elements(self, worker: str):
        worker_data = self._completed_data_cache[worker]
        del worker_data["y"][-1]
        del worker_data["x"][-1]
        del worker_data["marker"]["color"][-1]
        del worker_data["marker"]["pattern"]["shape"][-1]
        del worker_data["marker"]["line"]["color"][-1]
        del worker_data["marker"]["line"]["width"][-1]
        del worker_data["hovertext"][-1]
        del worker_data["hovertemplate"][-1]
        del worker_data["customdata"][-1]

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

    def __add_task_to_chart(self, worker: str, task_id: bytes, task_state: TaskState, task_time: float):
        task_color = self.__get_task_color(task_id)
        task_shape = TaskShapes.from_status(task_state)
        task_outline_color, task_outline_width = TaskShapes.get_outline(task_state)
        task_hovertext = self._worker_to_object_name.get(worker, "")
        capabilities_display_string = self._task_id_to_printable_capabilities.get(task_id, "")

        self.__add_bar(
            worker=worker,
            time_taken=task_time,
            task_color=task_color,
            task_outline_color=task_outline_color,
            task_outline_width=task_outline_width,
            shape=task_shape,
            hovertext=task_hovertext,
            capabilities_display_string=capabilities_display_string,
        )

    def __add_bar(
        self,
        worker: str,
        time_taken: float,
        task_color: str,
        task_outline_color: str,
        task_outline_width: int,
        shape: str,
        hovertext: str,
        capabilities_display_string: str,
    ):
        worker_history = self._completed_data_cache[worker]
        if len(worker_history["y"]) > 1:
            last_time_taken, last_color, last_text, last_shape = self.__get_history_fields(worker, -1)

            # lengthen last bar if they're the same type
            if last_color == task_color and last_text == hovertext and last_shape == shape:
                worker_history["x"][-1] += time_taken
                return

            # if there's a short gap from last task to current task, merge the bars
            # this serves two purposes:
            #   - get a clean bar instead of many ~0 width lines
            #   - more importantly, make the ui significantly more responsive
            if task_color != TASK_STREAM_BACKGROUND_COLOR and len(worker_history["y"]) > 2:
                _, penult_color, penult_text, penult_shape = self.__get_history_fields(worker, -2)

                if (
                    last_time_taken < 0.1
                    and penult_color == task_color
                    and penult_text == hovertext
                    and penult_shape == shape
                ):
                    worker_history["x"][-2] += time_taken + last_time_taken
                    self.__remove_last_elements(worker)
                    return

        self._completed_data_cache[worker]["y"].append(format_worker_name(worker))
        self._completed_data_cache[worker]["x"].append(time_taken)
        self._completed_data_cache[worker]["marker"]["color"].append(task_color)
        self._completed_data_cache[worker]["marker"]["pattern"]["shape"].append(shape)
        self._completed_data_cache[worker]["marker"]["line"]["color"].append(task_outline_color)
        self._completed_data_cache[worker]["marker"]["line"]["width"].append(task_outline_width)
        self._completed_data_cache[worker]["hovertext"].append(hovertext)
        self._completed_data_cache[worker]["customdata"].append(capabilities_display_string)

        if hovertext:
            self._completed_data_cache[worker]["hovertemplate"].append("%{hovertext} (%{x})<br>%{customdata}")
        else:
            self._completed_data_cache[worker]["hovertemplate"].append("")

    def __cutoff_keep_first(self, data_list: list, cutoff_index: int):
        return [data_list[0]] + data_list[cutoff_index:]

    def __remove_old_tasks_from_cache(self, worker: str, cutoff_index: int):
        worker_data = self._completed_data_cache[worker]
        removed_time = sum([worker_data["x"][i] for i in range(1, cutoff_index)])

        worker_data["y"] = self.__cutoff_keep_first(worker_data["y"], cutoff_index)
        worker_data["x"] = self.__cutoff_keep_first(worker_data["x"], cutoff_index)
        worker_data["marker"]["color"] = self.__cutoff_keep_first(worker_data["marker"]["color"], cutoff_index)
        worker_data["marker"]["pattern"]["shape"] = self.__cutoff_keep_first(
            worker_data["marker"]["pattern"]["shape"], cutoff_index
        )
        worker_data["marker"]["line"]["color"] = self.__cutoff_keep_first(
            worker_data["marker"]["line"]["color"], cutoff_index
        )
        worker_data["marker"]["line"]["width"] = self.__cutoff_keep_first(
            worker_data["marker"]["line"]["width"], cutoff_index
        )
        worker_data["hovertext"] = self.__cutoff_keep_first(worker_data["hovertext"], cutoff_index)
        worker_data["hovertemplate"] = self.__cutoff_keep_first(worker_data["hovertemplate"], cutoff_index)

        worker_data["x"][0] += removed_time

    def __handle_task_result(self, state: StateTask, now: datetime.datetime):
        worker = self._task_id_to_worker.get(state.task_id, "")
        if worker == "":
            return

        self._worker_last_update[worker] = now

        _, _, start = self._current_tasks.get(worker, (False, set(), None))

        if start is None:
            # we don't know when this task started, so just ignore
            return

        task_state = state.state
        task_time = format_timediff(start, now)

        with self._data_update_lock:
            self.__remove_task_from_worker(worker=worker, task_id=state.task_id, now=now, force_new_time=True)
        self.__add_task_to_chart(worker, state.task_id, task_state, task_time)

        self._task_id_to_printable_capabilities.pop(state.task_id)

    def __handle_new_worker(self, worker: str, now: datetime.datetime):
        if worker not in self._completed_data_cache:
            self.__setup_worker_cache(worker)
            self.__add_bar(
                worker=worker,
                time_taken=format_timediff(self._start_time, now),
                task_color=TASK_STREAM_BACKGROUND_COLOR,
                task_outline_color="rgba(0,0,0,0)",
                task_outline_width=0,
                shape=TaskShape.NONE.value,
                hovertext="",
                capabilities_display_string="",
            )
        self._seen_workers.add(worker)

    def __remove_task_from_worker(self, worker: str, task_id: bytes, now: datetime.datetime, force_new_time: bool):
        _, task_list, prev_start_time = self._current_tasks[worker]

        task_list.remove(task_id)

        self._current_tasks[worker] = (len(task_list) != 0, task_list, now if force_new_time else prev_start_time)

    def __handle_running_task(self, state_task: StateTask, worker: str, now: datetime.datetime):
        if state_task.task_id not in self._task_id_to_printable_capabilities:
            self._task_id_to_printable_capabilities[state_task.task_id] = display_capabilities(state_task.capabilities)

        # if another worker was previously assigned this task, remove it
        previous_worker = self._task_id_to_worker.get(state_task.task_id)
        if previous_worker and previous_worker != worker:
            self.__remove_task_from_worker(
                worker=previous_worker, task_id=state_task.task_id, now=now, force_new_time=False
            )

        self._task_id_to_worker[state_task.task_id] = worker
        self._worker_to_object_name[worker] = state_task.function_name.decode()

        doing_job, _, start_time = self._current_tasks.get(worker, (False, set(), None))
        if doing_job:
            with self._data_update_lock:
                self._current_tasks[worker][1].add(state_task.task_id)
                return

        with self._data_update_lock:
            self._current_tasks[worker] = (True, {state_task.task_id}, now)
        if start_time:
            self.__add_bar(
                worker=worker,
                time_taken=format_timediff(start_time, now),
                task_color=TASK_STREAM_BACKGROUND_COLOR,
                task_outline_color="rgba(0,0,0,0)",
                task_outline_width=0,
                shape=TaskShape.NONE.value,
                hovertext="",
                capabilities_display_string="",
            )

    def handle_task_state(self, state_task: StateTask):
        """
        The scheduler sends out `state.worker` while a Task is running.
        However, as soon as the task is done, that entry is cleared.
        A Success status will thus come with an empty `state.worker`, so
        we store this mapping ourselves based on the Running statuses we see.
        """

        task_state = state_task.state
        now = datetime.datetime.now()
        self._last_task_tick = now

        if task_state in COMPLETED_TASK_STATUSES:
            self.__handle_task_result(state_task, now)
            return

        if not (worker := state_task.worker):
            return

        worker_string = worker.decode()
        self._worker_last_update[worker_string] = now

        if worker_string not in self._seen_workers:
            self.__handle_new_worker(worker_string, now)

        if task_state in {TaskState.Running}:
            self.__handle_running_task(state_task, worker_string, now)

    def __add_lost_worker(self, worker: str, now: datetime.datetime):
        self._lost_workers_queue.put((now, worker))

    def __detect_lost_workers(self, now: datetime.datetime):
        removed_workers = []
        for worker in self._current_tasks.keys():
            last_tick = self._worker_last_update[worker]
            if now - last_tick > self._settings.memory_store_time:
                self.__add_lost_worker(worker, now)
                removed_workers.append(worker)

        for worker in removed_workers:
            self._current_tasks.pop(worker)

    def __remove_worker_from_history(self, worker: str):
        if worker in self._completed_data_cache:
            self._completed_data_cache.pop(worker)
            self._seen_workers.remove(worker)

    def __remove_old_tasks_from_history(self, store_duration: datetime.timedelta):
        for worker in self._completed_data_cache.keys():
            worker_data = self._completed_data_cache[worker]

            storage_cutoff_index = len(worker_data["x"]) - 1
            time_taken = 0
            store_seconds = store_duration.total_seconds()
            while storage_cutoff_index > 0 and time_taken < store_seconds:
                time_taken += worker_data["x"][storage_cutoff_index]
                storage_cutoff_index -= 1
            if storage_cutoff_index > 0:
                self.__remove_old_tasks_from_cache(worker, storage_cutoff_index)

    def __remove_old_workers(self, remove_up_to: datetime.datetime):
        while not self._lost_workers_queue.empty():
            timestamp, worker = self._lost_workers_queue.get()
            if timestamp > remove_up_to:
                self._lost_workers_queue.put((timestamp, worker))
                return
            self.__remove_worker_from_history(worker)

    def __remove_dead_workers(self, remove_up_to: datetime.datetime):
        while self._dead_workers and self._dead_workers[0][0] < remove_up_to:
            _, worker = self._dead_workers.popleft()
            self.__remove_worker_from_history(worker)

    def __split_workers_by_status(self, now: datetime.datetime) -> List[Tuple[str, float, bytes, str]]:
        workers_doing_jobs = []
        for worker, (doing_job, task_list, start_time) in self._current_tasks.items():
            if doing_job:
                worker_name = format_worker_name(worker)
                duration = format_timediff(start_time, now)
                object_name = self._worker_to_object_name.get(worker, "")

                # If a worker is doing multiple tasks, can we know which task it's currently working on?
                # We color based on the task's capabilities, but if we picked the wrong task this might be misleading.
                task = list(task_list)[0] if len(task_list) > 0 else b""
                workers_doing_jobs.append((worker_name, duration, task, object_name))
        return workers_doing_jobs

    def mark_dead_worker(self, worker_name: str):
        now = datetime.datetime.now()
        with self._data_update_lock:
            self._dead_workers.append((now, worker_name))

    def update_data(self, workers_section: WorkersSection):
        now = datetime.datetime.now()
        worker_names = sorted(workers_section.workers.keys())
        itls = {w: workers_section.workers[w].itl for w in worker_names}
        busy_workers = {w for w in worker_names if len(itls[w]) == 3 and itls[w][1] == "1" and itls[w][2] == "1"}
        for worker in worker_names:
            self._worker_last_update[worker] = now

        with self._data_update_lock:
            self._busy_workers = busy_workers
            self._busy_workers_update_time = now

    def clear_stale_busy_workers(self, now: datetime.datetime):
        if now - self._busy_workers_update_time > datetime.timedelta(seconds=2):
            self._busy_workers = set()

    def update_plot(self):
        with self._data_update_lock:
            now = datetime.datetime.now()

            self.clear_stale_busy_workers(now)

            task_update_time = self._last_task_tick
            workers_doing_tasks = self.__split_workers_by_status(now)

            self.__detect_lost_workers(now)
            worker_history_time = now - self._settings.memory_store_time
            self.__remove_old_workers(worker_history_time)
            self.__remove_old_tasks_from_history(self._settings.memory_store_time)

            worker_retention_time = now - self._settings.worker_retention_time
            self.__remove_dead_workers(worker_retention_time)

            completed_cache_values = list(self._completed_data_cache.values())

        if now - task_update_time >= datetime.timedelta(seconds=30):
            # get rid of the in-progress plots in ['data']
            self._figure["data"] = completed_cache_values
            self.__render_plot(now)
            return

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
                "line": {"color": ["yellow" for _ in workers_doing_tasks], "width": [2 for _ in workers_doing_tasks]},
            },
            "textfont": {"color": "black", "outline": "white", "outlinewidth": 5},
            "customdata": task_capabilities,
            "showlegend": False,
        }
        plot_data = completed_cache_values + [working_data]
        self._figure["data"] = plot_data
        self.__render_plot(now)

    def __render_plot(self, now: datetime.datetime):
        lower_bound, upper_bound = get_bounds(now, self._start_time, self._settings)

        ticks = make_ticks(lower_bound, upper_bound)
        tick_text = make_tick_text(int(self._settings.stream_window.total_seconds()))

        self._figure["layout"]["xaxis"]["range"] = [lower_bound, upper_bound]
        self._figure["layout"]["xaxis"]["tickvals"] = ticks
        self._figure["layout"]["xaxis"]["ticktext"] = tick_text
        self._plot.update()
