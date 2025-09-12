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
    DEFAULT_LOGGING_LEVEL,
    DEFAULT_LOGGING_PATHS,
)
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.config import ScalerConfig
from scaler.utility.event_loop import EventLoopType
from scaler.utility.network_util import get_available_tcp_port
from scaler.config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig


def get_args():
    parser = argparse.ArgumentParser("scaler scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML configuration file.")
    parser.add_argument("--zmq-io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
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
        default=DEFAULT_LOGGING_PATHS,
        help="specify where scheduler log should logged to, it can accept multiple files, default is /dev/stdout",
    )
    parser.add_argument(
        "--logging-level",
        "-ll",
        type=str,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default=DEFAULT_LOGGING_LEVEL,
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
        type=str,
        default=None,
        help="specify the object storage server address, if not specified, the address is scheduler address with port "
        "number plus 1, e.g.: if scheduler address is tcp://localhost:2345, then object storage address is "
        "tcp://localhost:2346",
    )
    parser.add_argument(
        "--monitor-address",
        "-ma",
        type=str,
        default=None,
        help="specify monitoring address, if not specified, the monitoring address is scheduler address with port "
        "number plus 2, e.g.: if scheduler address is tcp://localhost:2345, then monitoring address is "
        "tcp://localhost:2347",
    )
    parser.add_argument(
        "--adapter-webhook-url",
        "-awu",
        type=str,
        default=None,
        help="specify the adapter webhook url, if not specified, the adapter will not be used",
    )
    parser.add_argument(
        "address", type=ZMQConfig.from_string, help="scheduler address to connect to, e.g.: `tcp://localhost:6378`"
    )
    return parser.parse_args()


def main():
    args = get_args()

    scaler_config = ScalerConfig.get_config(args.config, args)

    if args.address is None:
        object_storage_address = ObjectStorageConfig(scaler_config.scheduler.address.host, get_available_tcp_port())
        object_storage = ObjectStorageServerProcess(
            storage_address=object_storage_address,
            logging_paths=scaler_config.logging.paths,
            logging_config_file=scaler_config.logging.config_file,
            logging_level=scaler_config.logging.level,
        )
        object_storage.start()
        object_storage.wait_until_ready()  # object storage should be ready before starting the cluster
    else:
        object_storage_address = args.object_storage_address
        object_storage = None

    scheduler = SchedulerProcess(
        address=scaler_config.scheduler.address,
        storage_address=object_storage_address,
        monitor_address=scaler_config.scheduler.monitor_address,
        adapter_webhook_url=scaler_config.scheduler.adapter_webhook_url,
        io_threads=scaler_config.scheduler.zmq_io_threads,
        max_number_of_tasks_waiting=scaler_config.scheduler.max_number_of_tasks_waiting,
        client_timeout_seconds=scaler_config.scheduler.client_timeout_seconds,
        worker_timeout_seconds=scaler_config.scheduler.worker_timeout_seconds,
        object_retention_seconds=scaler_config.scheduler.object_retention_seconds,
        load_balance_seconds=scaler_config.scheduler.load_balance_seconds,
        load_balance_trigger_times=scaler_config.scheduler.load_balance_trigger_times,
        protected=scaler_config.scheduler.protected,
        allocate_policy=scaler_config.scheduler.allocate_policy,
        event_loop=scaler_config.scheduler.event_loop,
        logging_paths=scaler_config.logging.paths,
        logging_config_file=scaler_config.logging.config_file,
        logging_level=scaler_config.logging.level,
    )
    scheduler.start()

    scheduler.join()
    if object_storage is not None:
        object_storage.join()
