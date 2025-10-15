import argparse

from scaler.config.loader import load_config
from scaler.cluster.object_storage_server import ObjectStorageServerProcess
from scaler.cluster.scheduler import SchedulerProcess
from scaler.scheduler.allocate_policy.allocate_policy import AllocatePolicy
from scaler.config.section.scheduler import SchedulerConfig
from scaler.config.types.object_storage_server import ObjectStorageConfig
from scaler.utility.event_loop import EventLoopType
from scaler.utility.network_util import get_available_tcp_port


def get_args():
    parser = argparse.ArgumentParser("scaler_scheduler", formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("--config", "-c", type=str, default=None, help="Path to the TOML configuration file.")
    parser.add_argument("--io-threads", type=int, help="number of io threads for zmq")
    parser.add_argument(
        "--max-number-of-tasks-waiting",
        "-mt",
        type=int,
        help="max number of tasks can wait in scheduler while all workers are full",
    )
    parser.add_argument("--client-timeout-seconds", "-ct", type=int, help="discard client when timeout seconds reached")
    parser.add_argument("--worker-timeout-seconds", "-wt", type=int, help="discard worker when timeout seconds reached")
    parser.add_argument(
        "--object-retention-seconds", "-ot", type=int, help="discard function in scheduler when timeout seconds reached"
    )
    parser.add_argument(
        "--load-balance-seconds", "-ls", type=int, help="number of seconds for load balance operation in scheduler"
    )
    parser.add_argument(
        "--load-balance-trigger-times",
        "-lbt",
        type=int,
        help="exact number of repeated load balance advices when trigger load balance operation in scheduler",
    )
    parser.add_argument("--event-loop", "-e", choices=EventLoopType.allowed_types(), help="select event loop type")
    parser.add_argument(
        "--protected", "-p", action="store_true", help="protect scheduler and worker from being shutdown by client"
    )
    parser.add_argument(
        "--allocate-policy",
        "-ap",
        choices=[p.name for p in AllocatePolicy],
        help="specify allocate policy, this controls how scheduler will prioritize tasks, including balancing tasks",
    )
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        help="specify where scheduler log should logged to, it can accept multiple files, default is /dev/stdout",
    )
    parser.add_argument("--logging-level", "-ll", type=str, help="specify the logging level")
    parser.add_argument(
        "--logging-config-file",
        "-lc",
        type=str,
        help="use standard python the .conf file the specify python logging file configuration format, this will "
        "bypass --logging-path",
    )
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=str,
        help="specify the object storage server address, if not specified, the address is scheduler address with port "
        "number plus 1, e.g.: if scheduler address is tcp://localhost:2345, then object storage address is "
        "tcp://localhost:2346",
    )
    parser.add_argument(
        "--monitor-address",
        "-ma",
        type=str,
        help="specify monitoring address, if not specified, the monitoring address is scheduler address with port "
        "number plus 2, e.g.: if scheduler address is tcp://localhost:2345, then monitoring address is "
        "tcp://localhost:2347",
    )
    parser.add_argument(
        "--adapter-webhook-url",
        "-awu",
        type=str,
        help="specify the adapter webhook url, if not specified, the adapter will not be used",
    )
    parser.add_argument(
        "scheduler_address", nargs="?", type=str, help="scheduler address to connect to, e.g.: `tcp://localhost:6378`"
    )
    parser.add_argument("--transport-type", type=str, help="the transport type to use, for example 'ymq' or 'zmq'")
    return parser.parse_args()


def main():
    args = get_args()

    scheduler_config = load_config(SchedulerConfig, args.config, args, section_name="scheduler")

    object_storage_address = scheduler_config.object_storage_address
    object_storage = None

    if object_storage_address is None:
        object_storage_address = ObjectStorageConfig(
            host=scheduler_config.scheduler_address.host, port=get_available_tcp_port()
        )
        object_storage = ObjectStorageServerProcess(
            storage_address=object_storage_address,
            logging_paths=scheduler_config.logging_paths,
            logging_config_file=scheduler_config.logging_config_file,
            logging_level=scheduler_config.logging_level,
        )
        object_storage.start()
        object_storage.wait_until_ready()  # object storage should be ready before starting the cluster

    scheduler = SchedulerProcess(
        address=scheduler_config.scheduler_address,
        storage_address=object_storage_address,
        monitor_address=scheduler_config.monitor_address,
        adapter_webhook_url=scheduler_config.adapter_webhook_url,
        io_threads=scheduler_config.io_threads,
        max_number_of_tasks_waiting=scheduler_config.max_number_of_tasks_waiting,
        client_timeout_seconds=scheduler_config.client_timeout_seconds,
        worker_timeout_seconds=scheduler_config.worker_timeout_seconds,
        object_retention_seconds=scheduler_config.object_retention_seconds,
        load_balance_seconds=scheduler_config.load_balance_seconds,
        load_balance_trigger_times=scheduler_config.load_balance_trigger_times,
        protected=scheduler_config.protected,
        allocate_policy=scheduler_config.allocate_policy,
        event_loop=scheduler_config.event_loop,
        transport_type=scheduler_config.transport_type,
        logging_paths=scheduler_config.logging_paths,
        logging_config_file=scheduler_config.logging_config_file,
        logging_level=scheduler_config.logging_level,
    )
    scheduler.start()

    scheduler.join()
    if object_storage is not None:
        object_storage.join()
