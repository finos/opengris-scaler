"""This example demonstrates how to start a scheduler using the Python API."""

from scaler import Scheduler
from scaler.io.config import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.scheduler.config import SchedulerConfig
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.zmq_config import ZMQConfig


def main():
    # First we need a SchedulerConfig as the parameter to Scheduler's ctor.

    # scaler provides a set of default to use. Kindly follow the comments there for detailed explanations.
    # Note, these defaults aims to be a starting point. You should change the defaults according to your use case.
    # Arguments that you would change are most likely "event_loop", "io_threads", "protected", and
    # "per_worker_queue_size".
    config = SchedulerConfig(
        event_loop="builtin",  # Either "builtin", or "uvloop"
        address=ZMQConfig.from_string(f"tcp://127.0.0.1:{get_available_tcp_port()}"),
        io_threads=DEFAULT_IO_THREADS,  # Consider increasing this number if your workload is IO-heavy
        max_number_of_tasks_waiting=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        per_worker_queue_size=DEFAULT_PER_WORKER_QUEUE_SIZE,
        client_timeout_seconds=DEFAULT_CLIENT_TIMEOUT_SECONDS,
        worker_timeout_seconds=DEFAULT_WORKER_TIMEOUT_SECONDS,
        object_retention_seconds=DEFAULT_OBJECT_RETENTION_SECONDS,
        load_balance_seconds=DEFAULT_LOAD_BALANCE_SECONDS,
        load_balance_trigger_times=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        protected=False,  # When false, clients can shutdown the scheduler.
    )

    # Then we put config into Scheduler. Unlike Cluster, scheduler should always be long running. Therefore, we don't
    # provide API to close scheduler. The only way to shutdown a scheduler is through Client.shutdown, which shutdowns
    # the scheduler if "protected" member variable is set to False
    _ = Scheduler(config)


if __name__ == "__main__":
    main()
