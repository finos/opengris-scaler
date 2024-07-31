import datetime

from nicegui import ui

SLIDING_WINDOW_OPTIONS = {
    datetime.timedelta(minutes=5): "5",
    datetime.timedelta(minutes=10): "10",
    datetime.timedelta(minutes=30): "30",
}
MEMORY_STORE_TIME = max(SLIDING_WINDOW_OPTIONS.keys())

MEMORY_USAGE_SCALE_OPTIONS = {"log": "log", "linear": "linear"}


class Settings:
    def __init__(self):
        self.stream_window = datetime.timedelta(minutes=5)
        self.memory_store_time = MEMORY_STORE_TIME
        self.memory_usage_scale = "log"

    def draw_section(self):
        with ui.card().classes("w-fit").classes("q-mx-auto"):
            ui.label("Sliding Window Length").classes("q-mx-auto")
            ui.toggle(SLIDING_WINDOW_OPTIONS).bind_value(self, "stream_window")

        with ui.card().classes("w-fit").classes("q-mx-auto"):
            ui.label("Memory Store Time").classes("q-mx-auto")
            ui.toggle(SLIDING_WINDOW_OPTIONS).bind_value(self, "memory_store_time")

        with ui.card().classes("w-fit").classes("q-mx-auto"):
            ui.label("Memory Usage Scale").classes("q-mx-auto")
            ui.toggle(MEMORY_USAGE_SCALE_OPTIONS).bind_value(self, "memory_usage_scale")

    @staticmethod
    def max_window_size() -> datetime.timedelta:
        return max(SLIDING_WINDOW_OPTIONS.keys())
