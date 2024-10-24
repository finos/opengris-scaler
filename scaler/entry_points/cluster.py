import argparse
import socket

from scaler.cluster.cluster import Cluster
from scaler.io.config import (
    DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
    DEFAULT_HARD_PROCESSOR_SUSPEND,
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_NUMBER_OF_WORKER,
    DEFAULT_TASK_TIMEOUT_SECONDS,
    DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.zmq_config import ZMQConfig


def get_args():
    parser = argparse.ArgumentParser(
        "standalone compute cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--num-of-workers", "-n", type=int, default=DEFAULT_NUMBER_OF_WORKER, help="number of workers in cluster"
    )
    parser.add_argument(
        "--worker-names",
        "-wn",
        type=str,
        default=None,
        help="worker names to replace default worker names (host names), separate by comma",
    )
    parser.add_argument(
        "--heartbeat-interval",
        "-hi",
        type=int,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="number of seconds to send heartbeat interval",
    )
    parser.add_argument(
        "--task-timeout-seconds",
        "-tts",
        type=int,
        default=DEFAULT_TASK_TIMEOUT_SECONDS,
        help="number of seconds task treat as timeout and return an exception",
    )
    parser.add_argument(
        "--garbage-collect-interval-seconds",
        "-gc",
        type=int,
        default=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        help="garbage collect interval seconds",
    )
    parser.add_argument(
        "--death-timeout-seconds", "-ds", type=int, default=DEFAULT_WORKER_DEATH_TIMEOUT, help="death timeout seconds"
    )
    parser.add_argument(
        "--trim-memory-threshold-bytes",
        "-tm",
        type=int,
        default=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        help="number of bytes threshold to enable libc to trim memory",
    )
    parser.add_argument(
        "--event-loop", "-el", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument(
        "--io-threads", "-it", default=DEFAULT_IO_THREADS, help="specify number of io threads per worker"
    )
    parser.add_argument(
        "--hard-processor-suspend",
        "-hps",
        action="store_true",
        default=DEFAULT_HARD_PROCESSOR_SUSPEND,
        help=(
            "When set, suspends worker processors using the SIGTSTP signal instead of a synchronization event, "
            "fully halting computation on suspended tasks. Note that this may cause some tasks to fail if they "
            "do not support being paused at the OS level (e.g. tasks requiring active network connections)."
        )
    )
    parser.add_argument(
        "--log-hub-address", "-la", default=None, type=ZMQConfig.from_string, help="address for Worker send logs"
    )
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        default=("/dev/stdout",),
        help='specify where cluster log should logged to, it can be multiple paths, "/dev/stdout" is default for '
        "standard output, each worker will have its own log file with process id appended to the path",
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
        type=str,
        default=None,
        help="use standard python the .conf file the specify python logging file configuration format, this will "
        "bypass --logging-paths and --logging-level at the same time, and this will not work on per worker logging",
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    register_event_loop(args.event_loop)

    if args.worker_names is None:
        worker_names = [f"{socket.gethostname().split('.')[0]}" for _ in range(args.num_of_workers)]
    else:
        worker_names = args.worker_names.split(",")
        if len(worker_names) != args.num_of_workers:
            raise ValueError(
                f"number of worker names ({len(args.worker_names)}) must match number of workers "
                f"({args.num_of_workers})"
            )

    cluster = Cluster(
        address=args.address,
        worker_names=worker_names,
        heartbeat_interval_seconds=args.heartbeat_interval,
        task_timeout_seconds=args.task_timeout_seconds,
        garbage_collect_interval_seconds=args.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=args.trim_memory_threshold_bytes,
        death_timeout_seconds=args.death_timeout_seconds,
        hard_processor_suspend=args.hard_processor_suspend,
        event_loop=args.event_loop,
        worker_io_threads=args.io_threads,
        logging_paths=args.logging_paths,
        logging_level=args.logging_level,
        logging_config_file=args.logging_config_file,
    )
    cluster.run()
