import datetime
from typing import Any, Dict, Optional

from nicegui import ui

from scaler.protocol.python.message import StateTask
from scaler.ui.setting_page import Settings
from scaler.ui.utility import format_timediff, get_bounds, make_tick_text, make_ticks
from scaler.utility.formatter import format_bytes
from scaler.utility.metadata.profile_result import ProfileResult

CHART_NAME = "Memory Usage"
X_AXIS_GRID_LINES = False

Y_AXIS_TICK_VALS = [0, 1024, 1024 * 1024, 1024 * 1024 * 1024, 1024 * 1024 * 1024 * 1024]
Y_AXIS_TICK_TEXT = ["0", "1KB", "1MB", "1GB", "1TB"]
Y_AXIS_GRID_LINES = True


class MemoryChart:
    def __init__(self):
        self._figure = {}
        self._plot = None
        self._plot_data: Dict[str, Any] = {}

        self._settings: Optional[Settings] = None

        self._start_time = datetime.datetime.now() - datetime.timedelta(minutes=30)

    def setup_memory_chart(self, settings: Settings):
        with ui.card().classes("w-full").style("height: 30vh"):
            self._plot_data = {
                "type": "scatter",
                "fill": "tozeroy",
                "fillcolor": "rgba(0,0,255,1)",
                "mode": "none",
                "name": "",
                "x": [],
                "y": [],
                "hovertemplate": [],
            }
            fig = {
                "data": [self._plot_data],
                "layout": {
                    "title": {"text": CHART_NAME},
                    "autosize": True,
                    "margin": {"l": 163},
                    "xaxis": {
                        "autorange": False,
                        "range": [0, 300],
                        "showgrid": X_AXIS_GRID_LINES,
                        "tickmode": "array",
                        "tickvals": [0, 50, 100, 150, 200, 250, 300],
                        "ticktext": [-300, -250, -200, -150, -100, -50, 0],
                        "zeroline": False,
                    },
                    "yaxis": {
                        "tickvals": Y_AXIS_TICK_VALS,
                        "ticktext": Y_AXIS_TICK_TEXT,
                        "autorange": True,
                        "automargin": True,
                        "rangemode": "nonnegative",
                        "showgrid": Y_AXIS_GRID_LINES,
                        "type": "log",
                    },
                },
            }
            self._figure = fig
            self._plot = ui.plotly(self._figure).classes("w-full h-full")
            self._settings = settings

    def handle_task_state(self, state: StateTask):
        """
        Only completed tasks have profiling data.
        Use this data to fill in history.
        """

        if state.metadata == b"":
            return

        profile_result = ProfileResult.deserialize(state.metadata)

        worker_memory = profile_result.memory_peak
        worker_duration = profile_result.duration_s

        if worker_memory == 0:
            return

        self.__add_memory_usage(worker_duration, worker_memory)

    def update_plot(self):
        now = datetime.datetime.now()
        self.__render_plot(now)

    def __add_memory_usage(self, time_taken: float, memory_usage: int):
        now = datetime.datetime.now()
        current_time = format_timediff(self._start_time, now)
        task_start_time = current_time - time_taken

        # find index we need to start changing memory use from
        insert_index = len(self._plot_data["x"])
        while insert_index > 0 and self._plot_data["x"][insert_index - 1] > task_start_time:
            insert_index -= 1

        # insert points to mark change
        self._plot_data["x"].insert(insert_index, task_start_time)
        self._plot_data["x"].insert(insert_index, task_start_time - 0.01)

        prev_mem = 0
        if insert_index < len(self._plot_data["y"]):
            prev_mem = self._plot_data["y"][insert_index]

        self._plot_data["y"].insert(insert_index, prev_mem + memory_usage)
        self._plot_data["y"].insert(insert_index, prev_mem)

        self._plot_data["hovertemplate"].insert(insert_index, format_bytes(prev_mem + memory_usage))
        self._plot_data["hovertemplate"].insert(insert_index, format_bytes(prev_mem))

        # fill future overlapping points with the additional memory use
        i = insert_index + 2
        while i < len(self._plot_data["x"]):
            self._plot_data["y"][i] += memory_usage
            self._plot_data["hovertemplate"][i] = format_bytes(self._plot_data["y"][i])
            i += 1

        # mark end of this task's memory use. always going to be the latest task we have so far.
        self._plot_data["y"].append(memory_usage)
        self._plot_data["hovertemplate"].append(format_bytes(memory_usage))
        self._plot_data["x"].append(current_time - 0.01)

        self._plot_data["y"].append(0)
        self._plot_data["hovertemplate"].append(format_bytes(0))
        self._plot_data["x"].append(current_time)

    def __render_plot(self, now: datetime.datetime):
        lower_bound, upper_bound = get_bounds(now, self._start_time, self._settings)

        ticks = make_ticks(lower_bound, upper_bound)
        tick_text = make_tick_text(int(self._settings.stream_window.total_seconds()))

        self._figure["layout"]["xaxis"]["range"] = [lower_bound, upper_bound]
        self._figure["layout"]["xaxis"]["tickvals"] = ticks
        self._figure["layout"]["xaxis"]["ticktext"] = tick_text
        self._figure["layout"]["yaxis"]["type"] = self._settings.memory_usage_scale

        self._plot.update()
