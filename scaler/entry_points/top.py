import argparse
import curses
import functools
from typing import Dict, List, Literal, Union

from scaler.io.sync_subscriber import SyncSubscriber
from scaler.protocol.python.message import StateScheduler
from scaler.protocol.python.mixins import Message
from scaler.utility.formatter import (
    format_bytes,
    format_integer,
    format_microseconds,
    format_percentage,
    format_seconds,
)
from scaler.utility.zmq_config import ZMQConfig

SORT_BY_OPTIONS = {
    ord("n"): "worker",
    ord("C"): "agt_cpu",
    ord("M"): "agt_rss",
    ord("c"): "cpu",
    ord("m"): "rss",
    ord("F"): "rss_free",
    ord("f"): "free",
    ord("w"): "sent",
    ord("d"): "queued",
    ord("s"): "suspended",
    ord("l"): "lag",
}

SORT_BY_STATE: Dict[str, Union[str, bool]] = {"sort_by": "cpu", "sort_by_previous": "cpu", "sort_reverse": True}


def get_args():
    parser = argparse.ArgumentParser(
        "monitor scheduler as top like", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--timeout", "-t", type=int, default=5, help="timeout seconds")
    parser.add_argument("address", help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    curses.wrapper(poke, args)


def poke(screen, args):
    screen.nodelay(1)

    try:
        subscriber = SyncSubscriber(
            address=ZMQConfig.from_string(args.address),
            callback=functools.partial(show_status, screen=screen),
            topic=b"",
            daemonic=False,
            timeout_seconds=args.timeout,
        )
        subscriber.run()
    except KeyboardInterrupt:
        pass


def show_status(status: Message, screen):
    if not isinstance(status, StateScheduler):
        return

    __change_option_state(screen.getch())

    scheduler_table = __generate_keyword_data(
        "scheduler",
        {
            "cpu": format_percentage(status.scheduler.cpu),
            "rss": format_bytes(status.scheduler.rss),
            "rss_free": format_bytes(status.rss_free),
        },
    )

    task_manager_table = __generate_keyword_data(
        "task_manager",
        {
            "unassigned": status.task_manager.unassigned,
            "running": status.task_manager.running,
            "success": status.task_manager.success,
            "failed": status.task_manager.failed,
            "canceled": status.task_manager.canceled,
            "not_found": status.task_manager.not_found,
        },
        format_integer_flag=True,
    )
    object_manager = __generate_keyword_data(
        "object_manager",
        {
            "num_of_objs": status.object_manager.number_of_objects,
            "obj_mem": format_bytes(status.object_manager.object_memory),
        },
    )
    sent_table = __generate_keyword_data("scheduler_sent", status.binder.sent, format_integer_flag=True)
    received_table = __generate_keyword_data("scheduler_received", status.binder.received, format_integer_flag=True)
    client_table = __generate_keyword_data(
        "client_manager", status.client_manager.client_to_num_of_tasks, key_col_length=18
    )
    worker_manager_table = __generate_worker_manager_table(
        [
            {
                "worker": worker.worker_id.decode(),
                "agt_cpu": worker.agent.cpu,
                "agt_rss": worker.agent.rss,
                "cpu": sum(p.resource.cpu for p in worker.processor_statuses),
                "rss": sum(p.resource.rss for p in worker.processor_statuses),
                "os_rss_free": worker.rss_free,
                "free": worker.free,
                "sent": worker.sent,
                "queued": worker.queued,
                "suspended": worker.suspended,
                "lag": worker.lag_us,
                "last": worker.last_s,
                "ITL": worker.itl,
            }
            for worker in status.worker_manager.workers
        ],
        worker_length=24,
    )

    table1 = __merge_tables(scheduler_table, object_manager, padding="|")
    table1 = __merge_tables(table1, task_manager_table, padding="|")
    table1 = __merge_tables(table1, sent_table, padding="|")
    table1 = __merge_tables(table1, received_table, padding="|")

    table3 = __merge_tables(worker_manager_table, client_table, padding="|")

    screen.clear()
    try:
        new_row, max_cols = __print_table(screen, 0, table1, padding=1)
    except curses.error:
        __print_too_small(screen)
        return

    try:
        screen.addstr(new_row, 0, "-" * max_cols)
        screen.addstr(new_row + 1, 0, "Shortcuts: " + " ".join([f"{v}[{chr(k)}]" for k, v in SORT_BY_OPTIONS.items()]))
        screen.addstr(new_row + 3, 0, f"Total {len(status.worker_manager.workers)} worker(s)")
        _ = __print_table(screen, new_row + 4, table3)
    except curses.error:
        pass

    screen.refresh()


def __generate_keyword_data(title, data, key_col_length: int = 0, format_integer_flag: bool = False):
    table = [[title, ""]]

    def format_integer_func(value):
        if format_integer_flag:
            return format_integer(value)

        return value

    table.extend([[__truncate(k, key_col_length), format_integer_func(v)] for k, v in data.items()])
    return table


def __generate_worker_manager_table(wm_data: List[Dict], worker_length: int) -> List[List[str]]:
    if not wm_data:
        headers = [["No workers"]]
        return headers

    wm_data = sorted(
        wm_data, key=lambda item: item[SORT_BY_STATE["sort_by"]], reverse=bool(SORT_BY_STATE["sort_reverse"])
    )

    for row in wm_data:
        row["worker"] = __truncate(row["worker"], worker_length, how="left")
        row["agt_cpu"] = format_percentage(row["agt_cpu"])
        row["agt_rss"] = format_bytes(row["agt_rss"])
        row["cpu"] = format_percentage(row["cpu"])
        row["rss"] = format_bytes(row["rss"])
        row["os_rss_free"] = format_bytes(row["os_rss_free"])

        last = row.pop("last")
        last = f"({format_seconds(last)}) " if last > 5 else ""
        row["lag"] = last + format_microseconds(row["lag"])

    worker_manager_table = [[f"[{v}]" if v == SORT_BY_STATE["sort_by"] else v for v in wm_data[0].keys()]]
    worker_manager_table.extend([list(worker.values()) for worker in wm_data])
    return worker_manager_table


def __print_table(screen, line_number, data, padding: int = 1):
    if not data:
        return

    col_widths = [max(len(str(row[i])) for row in data) for i in range(len(data[0]))]

    for i, header in enumerate(data[0]):
        screen.addstr(line_number, sum(col_widths[:i]) + (padding * i), str(header).rjust(col_widths[i]))

    for i, row in enumerate(data[1:], start=1):
        for j, cell in enumerate(row):
            screen.addstr(line_number + i, sum(col_widths[:j]) + (padding * j), str(cell).rjust(col_widths[j]))

    return line_number + len(data), sum(col_widths) + (padding * len(col_widths))


def __merge_tables(left: List[List], right: List[List], padding: str = "") -> List[List]:
    if not left:
        return right

    if not right:
        return left

    result = []
    for i in range(max(len(left), len(right))):
        if i < len(left):
            left_row = left[i]
        else:
            left_row = [""] * len(left[0])

        if i < len(right):
            right_row = right[i]
        else:
            right_row = [""] * len(right[0])

        if padding:
            padding_column = [padding]
            result.append(left_row + padding_column + right_row)
        else:
            result.append(left_row + right_row)

    return result


def __concat_tables(up: List[List], down: List[List], padding: int = 1) -> List[List]:
    max_cols = max([len(row) for row in up] + [len(row) for row in down])
    for row in up:
        row.extend([""] * (max_cols - len(row)))

    padding_rows = [[""] * max_cols] * padding

    for row in down:
        row.extend([""] * (max_cols - len(row)))

    return up + padding_rows + down


def __truncate(string: str, number: int, how: Literal["left", "right"] = "left") -> str:
    if number <= 0:
        return string

    if len(string) <= number:
        return string

    if how == "left":
        return f"{string[:number]}+"
    else:
        return f"+{string[-number:]}"


def __print_too_small(screen):
    screen.clear()
    screen.addstr(0, 0, "Your terminal is too small to show")
    screen.refresh()


def __change_option_state(option: int):
    if option not in SORT_BY_OPTIONS.keys():
        return

    SORT_BY_STATE["sort_by_previous"] = SORT_BY_STATE["sort_by"]
    SORT_BY_STATE["sort_by"] = SORT_BY_OPTIONS[option]
    if SORT_BY_STATE["sort_by"] != SORT_BY_STATE["sort_by_previous"]:
        SORT_BY_STATE["sort_reverse"] = True
        return

    SORT_BY_STATE["sort_reverse"] = not SORT_BY_STATE["sort_reverse"]
