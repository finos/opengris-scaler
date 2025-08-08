import argparse

from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.io.config import (
    DEFAULT_CLIENT_TIMEOUT_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_LOAD_BALANCE_SECONDS,
    DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
    DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
    DEFAULT_OBJECT_RETENTION_SECONDS,
    DEFAULT_WORKER_TIMEOUT_SECONDS,
)
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.utility.event_loop import EventLoopType
from scaler.utility.network_util import get_available_tcp_port
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.ymq_config import YMQConfig


def get_args():
    parser = argparse.ArgumentParser("scaler scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
    parser.add_argument(
        "--max-number-of-tasks-waiting",
        "-mt",
        type=int,
        default=DEFAULT_MAX_NUMBER_OF_TASKS_WAITING,
        help="max number of tasks can wait in scheduler while all workers are full",
    )
    parser.add_argument(
        "--client-timeout-seconds",
        "-ct",
        type=int,
        default=DEFAULT_CLIENT_TIMEOUT_SECONDS,
        help="discard client when timeout seconds reached",
    )
    parser.add_argument(
        "--worker-timeout-seconds",
        "-wt",
        type=int,
        default=DEFAULT_WORKER_TIMEOUT_SECONDS,
        help="discard worker when timeout seconds reached",
    )
    parser.add_argument(
        "--object-retention-seconds",
        "-ot",
        type=int,
        default=DEFAULT_OBJECT_RETENTION_SECONDS,
        help="discard function in scheduler when timeout seconds reached",
    )
    parser.add_argument(
        "--load-balance-seconds",
        "-ls",
        type=int,
        default=DEFAULT_LOAD_BALANCE_SECONDS,
        help="number of seconds for load balance operation in scheduler",
    )
    parser.add_argument(
        "--load-balance-trigger-times",
        "-lbt",
        type=int,
        default=DEFAULT_LOAD_BALANCE_TRIGGER_TIMES,
        help="exact number of repeated load balance advices when trigger load balance operation in scheduler",
    )
    parser.add_argument(
        "--event-loop", "-e", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument(
        "--protected", "-p", action="store_true", help="protect scheduler and worker from being shutdown by client"
    )
    parser.add_argument(
        "--store-tasks",
        "-k",
        action="store_true",
        help="store tasks in scheduler when tasks are running, if this is true, when worker disconnected, all tasks "
        "on this worker will get rerouted to other workers",
    )
    parser.add_argument(
        "--allocate-policy",
        "-ap",
        choices=[p.name for p in AllocatePolicy],
        default=AllocatePolicy.even.name,
        help="specify allocate policy, this controls how scheduler will prioritize tasks, including balancing tasks",
    )
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        default=("/dev/stdout",),
        help="specify where scheduler log should logged to, it can accept multiple files, default is /dev/stdout",
    )
    parser.add_argument(
        "--logging-level",
        "-ll",
        type=str,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
        help="specify the logging level",
    )
    parser.add_argument(
        "--logging-config-file",
        "-lc",
        type=str,
        default=None,
        help="use standard python the .conf file the specify python logging file configuration format, this will "
        "bypass --logging-path",
    )
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=ObjectStorageConfig.from_string,
        default=None,
        help="specify the object storage server address, if not specified, the address is scheduler address with port "
        "number plus 1, e.g.: if scheduler address is tcp://localhost:2345, then object storage address is "
        "tcp://localhost:2346",
    )
    parser.add_argument(
        "--monitor-address",
        "-ma",
        type=YMQConfig.from_string,
        default=None,
        help="specify monitoring address, if not specified, the monitoring address is scheduler address with port "
        "number plus 2, e.g.: if scheduler address is tcp://localhost:2345, then monitoring address is "
        "tcp://localhost:2347",
    )
    parser.add_argument(
        "address",
        type=YMQConfig.from_string,
        help="scheduler address to connect to, e.g.: `tcp://localhost:6378`"
    )
    return parser.parse_args()


def main():
    args = get_args()

    if args.object_storage_address is None:
        object_storage_address = ObjectStorageConfig(args.address.host, get_available_tcp_port())
        object_storage = ObjectStorageServerProcess(
            storage_address=object_storage_address,
            logging_paths=args.logging_paths,
            logging_config_file=args.logging_config_file,
            logging_level=args.logging_level,
        )
        object_storage.start()
        object_storage.wait_until_ready()  # object storage should be ready before starting the cluster
    else:
        object_storage_address = args.object_storage_address

    scheduler = SchedulerProcess(
        address=args.address,
        storage_address=object_storage_address,
        monitor_address=args.monitor_address,
        io_threads=args.io_threads,
        max_number_of_tasks_waiting=args.max_number_of_tasks_waiting,
        client_timeout_seconds=args.client_timeout_seconds,
        worker_timeout_seconds=args.worker_timeout_seconds,
        object_retention_seconds=args.object_retention_seconds,
        load_balance_seconds=args.load_balance_seconds,
        load_balance_trigger_times=args.load_balance_trigger_times,
        protected=args.protected,
        store_tasks=args.store_tasks,
        allocate_policy=AllocatePolicy[args.allocate_policy],
        event_loop=args.event_loop,
        logging_paths=args.logging_paths,
        logging_config_file=args.logging_config_file,
        logging_level=args.logging_level,
    )
    scheduler.start()

    scheduler.join()
    object_storage.join()
