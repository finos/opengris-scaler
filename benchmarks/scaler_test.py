import logging
import random

from scaler.client.client import Client
from scaler.cluster.combo import SchedulerClusterCombo
from scaler.utility.logging.scoped_logger import ScopedLogger
from scaler.utility.logging.utility import setup_logger, get_logger_info


def sleep_print(sec: int):
    return sec * 1


def main():
    setup_logger()
    log_format, log_level_str, log_path = get_logger_info(logging.getLogger())

    address = "tcp://127.0.0.1:2345"

    cluster = SchedulerClusterCombo(
        address=address,
        n_workers=10,
        per_worker_task_queue_size=2,
        event_loop="builtin",
        logging_paths=log_path,
        logging_level=log_level_str,
        logging_format=log_format,
    )
    client = Client(address=address)

    tasks = [random.randint(0, 101) for _ in range(10000)]

    with ScopedLogger(f"Scaler submit {len(tasks)} tasks"):
        futures = [client.submit(sleep_print, a) for a in tasks]

    with ScopedLogger(f"Scaler gather {len(futures)} results"):
        results = [future.result() for future in futures]

    assert results == tasks

    client.disconnect()
    cluster.shutdown()


if __name__ == "__main__":
    main()
