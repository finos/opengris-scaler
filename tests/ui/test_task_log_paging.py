"""The task list and the task log, both paged server-side.

A browser is sent one page whatever the GUI retains.
Raising the retention grows what an operator can page back through, not what crosses the socket.
"""

import unittest

from scaler.config.types.address import AddressConfig
from scaler.protocol.capnp import StateBalanceAdvice, StateTask, TaskState
from scaler.ui.app import TASK_EVENTS_PAGE_SIZE, TASK_LOG_PAGE_SIZE, BrowserView, WebGUIConfig, WebUIApp, _RenderCache
from scaler.utility.metadata.profile_result import ProfileResult


def make_app(retained: int = 1000) -> WebUIApp:
    config = WebGUIConfig(monitor_address=AddressConfig.from_string("tcp://127.0.0.1:6380"), task_log_max_size=retained)
    return WebUIApp(config)


def make_task(**kwargs) -> StateTask:
    """A StateTask as the GUI receives it: capability reads need a deserialized struct."""
    return StateTask.from_bytes(StateTask(**kwargs).to_bytes())


def run_tasks(app: WebUIApp, count: int, state: TaskState = TaskState.success) -> None:
    for index in range(count):
        task_id = index.to_bytes(32, "big")
        app._process_task_state(make_task(taskId=task_id, functionName=b"work", state=TaskState.running, worker=b"w1"))
        app._record_task_event(make_task(taskId=task_id, functionName=b"work", state=TaskState.running, worker=b"w1"))
        app._process_task_state(make_task(taskId=task_id, functionName=b"work", state=state, worker=b"w1"))
        app._record_task_event(make_task(taskId=task_id, functionName=b"work", state=state, worker=b"w1"))


class TestTaskListPaging(unittest.TestCase):
    def test_a_browser_is_sent_one_page_however_many_tasks_are_retained(self) -> None:
        app = make_app()
        run_tasks(app, 300)

        section = app._task_log_section(BrowserView(), _RenderCache())
        self.assertEqual(len(section["task_log"]), TASK_LOG_PAGE_SIZE)
        self.assertEqual(section["task_log_held"], 300)
        self.assertEqual(section["task_log_total"], 300)
        self.assertEqual(section["task_log_pages"], 6)

    def test_the_second_page_carries_the_next_rows(self) -> None:
        app = make_app()
        run_tasks(app, 300)

        first = app._task_log_section(BrowserView(), _RenderCache())["task_log"]
        second = app._task_log_section(BrowserView(task_log_page=1), _RenderCache())["task_log"]
        self.assertEqual([row["task_id"] for row in first[:1]], [(299).to_bytes(32, "big").hex()])
        self.assertEqual([row["task_id"] for row in second[:1]], [(249).to_bytes(32, "big").hex()])

    def test_a_page_past_the_end_is_clamped_to_the_last_one(self) -> None:
        app = make_app()
        run_tasks(app, 60)

        view = BrowserView(task_log_page=99)
        section = app._task_log_section(view, _RenderCache())
        self.assertEqual((section["task_log_page"], view.task_log_page), (1, 1))

    def test_a_task_keeps_one_row_across_its_whole_life(self) -> None:
        """Running then succeeding is one task, so the list must not grow a row per state change."""
        app = make_app()
        run_tasks(app, 1)

        rows = app._task_log_section(BrowserView(), _RenderCache())["task_log"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "success")
        self.assertEqual(rows[0]["worker"], "w1")

    def test_retention_bounds_the_rows_and_the_index_together(self) -> None:
        app = make_app(retained=100)
        run_tasks(app, 250)

        self.assertEqual(len(app._task_log), 100)
        self.assertEqual(len(app._task_log_by_id), 100)
        self.assertEqual(app._task_log_section(BrowserView(), _RenderCache())["task_log_total"], 250)


class TestTaskListFilters(unittest.TestCase):
    @staticmethod
    def submit(app: WebUIApp, index: int, client: bytes, worker: bytes, state: TaskState) -> None:
        task = make_task(
            taskId=index.to_bytes(32, "big"), functionName=b"work", state=state, worker=worker, client=client
        )
        app._process_task_state(task)

    def make_tasks(self) -> WebUIApp:
        """60 tasks of Client|a and 40 of Client|b, alternating workers, every tenth failed."""
        app = make_app()
        for index in range(100):
            client = b"Client|a" if index < 60 else b"Client|b"
            worker = b"Worker|one" if index % 2 else b"Worker|two"
            self.submit(app, index, client, worker, TaskState.failed if index % 10 == 0 else TaskState.success)
        return app

    def test_a_client_filter_pages_through_that_clients_tasks_alone(self) -> None:
        app = self.make_tasks()
        section = app._task_log_section(BrowserView(task_log_client="Client|b"), _RenderCache())

        self.assertEqual({row["full_client"] for row in section["task_log"]}, {"Client|b"})
        self.assertEqual(section["task_log_matched"], 40)
        self.assertEqual(section["task_log_pages"], 1)
        self.assertEqual(section["task_log_held"], 100, "the badge still counts everything the list holds")
        self.assertEqual(section["task_log_client"], "Client|b", "the browser mirrors the filter it was served")

    def test_filters_combine(self) -> None:
        app = self.make_tasks()
        view = BrowserView(task_log_client="Client|a", task_log_worker="Worker|two", task_log_status="failed")
        rows = app._task_log_section(view, _RenderCache())["task_log"]

        self.assertEqual(len(rows), 6)
        self.assertEqual({(row["full_worker"], row["status"]) for row in rows}, {("Worker|two", "failed")})

    def test_a_filtered_list_sorts_within_the_filter(self) -> None:
        app = self.make_tasks()
        view = BrowserView(task_log_status="failed", task_log_sort="task_id", task_log_sort_ascending=False)
        rows = app._task_log_section(view, _RenderCache())["task_log"]

        self.assertEqual(
            [row["task_id"] for row in rows[:2]], [(90).to_bytes(32, "big").hex(), (80).to_bytes(32, "big").hex()]
        )

    def test_two_browsers_with_different_filters_share_a_cache_without_mixing(self) -> None:
        app = self.make_tasks()
        cache = _RenderCache()
        first = app._task_log_section(BrowserView(task_log_client="Client|a", task_log_sort="task_id"), cache)
        second = app._task_log_section(BrowserView(task_log_client="Client|b", task_log_sort="task_id"), cache)

        self.assertEqual((first["task_log_matched"], second["task_log_matched"]), (60, 40))


class TestTaskLogPaging(unittest.TestCase):
    def test_a_browser_is_sent_one_page_of_events(self) -> None:
        app = make_app()
        run_tasks(app, 200)

        section = app._task_events_section(BrowserView(), _RenderCache())
        self.assertEqual(len(section["task_events"]), TASK_EVENTS_PAGE_SIZE)
        self.assertEqual(section["task_events_held"], 400)  # one queued and one success per task

    def test_filtering_to_one_task_shows_only_its_events(self) -> None:
        app = make_app()
        run_tasks(app, 200)
        wanted = (7).to_bytes(32, "big").hex()

        section = app._task_events_section(BrowserView(task_events_task=wanted), _RenderCache())
        self.assertEqual(section["task_events_held"], 2)
        self.assertEqual({row["task_id"] for row in section["task_events"]}, {wanted})
        self.assertEqual([row["status"] for row in section["task_events"]], ["success", "queued"])

    def test_a_result_still_names_the_worker_that_ran_the_task(self) -> None:
        """A result message carries no worker, so a bare "success" row would say nothing about where."""
        app = make_app()
        task_id = (1).to_bytes(32, "big")
        for state, worker in ((TaskState.running, b"w1"), (TaskState.success, b"")):
            task = make_task(taskId=task_id, functionName=b"work", state=state, worker=worker, client=b"Client|one")
            app._process_task_state(task)
            app._record_task_event(task)

        events = app._task_events_section(BrowserView(), _RenderCache())["task_events"]
        self.assertEqual([row["status"] for row in events], ["success", "queued"])
        self.assertEqual({row["worker"] for row in events}, {"w1"})
        self.assertEqual({row["client"] for row in events}, {"Client|one"})

    def test_a_rebalance_leaves_its_own_row(self) -> None:
        """The balancer's pick is not a transition: the scheduler may still refuse it, so the row sets no status."""
        app = make_app()
        run_tasks(app, 1)
        app._record_balance_advice(StateBalanceAdvice(workerId=b"w1", taskIds=[(0).to_bytes(32, "big")]))

        events = app._task_events_section(BrowserView(), _RenderCache())["task_events"]
        self.assertEqual([row["status"] for row in events], ["", "success", "queued"])
        self.assertEqual(events[0]["event"], "StateBalanceAdvice")

    def test_a_row_names_the_scheduler_event_that_made_it(self) -> None:
        app = make_app()
        task = make_task(
            taskId=b"t" * 32, state=TaskState.balanceCanceling, worker=b"w1", event="BalanceCancelRequested"
        )
        app._process_task_state(task)
        app._record_task_event(task)

        [row] = app._task_events_section(BrowserView(), _RenderCache())["task_events"]
        self.assertEqual((row["status"], row["event"]), ("balanceCanceling", "BalanceCancelRequested"))


class TestSortedTaskViews(unittest.TestCase):
    """Sorting runs on the server, so page one of a sorted table is the head of the whole retained set."""

    def test_the_longest_running_tasks_come_first_however_many_pages_there_are(self) -> None:
        app = make_app()
        for index in range(120):
            task_id = index.to_bytes(32, "big")
            app._process_task_state(make_task(taskId=task_id, state=TaskState.running, worker=b"w1"))
            app._process_task_state(
                make_task(
                    taskId=task_id,
                    state=TaskState.success,
                    worker=b"w1",
                    metadata=ProfileResult(duration_s=float(index), memory_peak=index).serialize(),
                )
            )

        view = BrowserView(task_log_sort="duration", task_log_sort_ascending=False)
        rows = app._task_log_section(view, _RenderCache())["task_log"]
        self.assertEqual(rows[0]["duration"], "119.00s")
        self.assertEqual([row["duration"] for row in rows[:3]], ["119.00s", "118.00s", "117.00s"])

    def test_one_sort_serves_every_browser_asking_for_it(self) -> None:
        app = make_app()
        run_tasks(app, 60)
        cache = _RenderCache()

        first = app._task_log_section(BrowserView(task_log_sort="task_id"), cache)["task_log"]
        second = app._task_log_section(BrowserView(task_log_sort="task_id", task_log_page=1), cache)["task_log"]
        self.assertEqual(first[0]["task_id"], (0).to_bytes(32, "big").hex())
        self.assertEqual(second[0]["task_id"], (50).to_bytes(32, "big").hex())

    def test_the_trail_orders_by_when_a_row_was_appended_not_by_its_clock_reading(self) -> None:
        """Every row of a busy second reads the same time, so the sequence is what orders them."""
        app = make_app()
        run_tasks(app, 3)

        view = BrowserView(task_events_sort="time", task_events_sort_ascending=True)
        events = app._task_events_section(view, _RenderCache())["task_events"]
        self.assertEqual([row["seq"] for row in events], sorted(row["seq"] for row in events))
        self.assertEqual(events[0]["task_id"], (0).to_bytes(32, "big").hex())

    def test_a_filtered_trail_sorts_within_the_filter(self) -> None:
        app = make_app()
        run_tasks(app, 5)
        wanted = (2).to_bytes(32, "big").hex()

        view = BrowserView(task_events_task=wanted, task_events_sort="status", task_events_sort_ascending=True)
        section = app._task_events_section(view, _RenderCache())
        self.assertEqual([row["status"] for row in section["task_events"]], ["queued", "success"])
        self.assertEqual(section["task_events_held"], 2)


if __name__ == "__main__":
    unittest.main()
