import argparse
import socket

from scaler.cluster.cluster import Cluster
from scaler.config.section.cluster import ClusterConfig
from scaler.config.loader import load_config
from scaler.utility.event_loop import EventLoopType, register_event_loop


def get_args():
    parser = argparse.ArgumentParser(
        "standalone compute cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", "-c", type=str, default=None, help="Path to the TOML configuration file.")

    parser.add_argument(
        "--preload",
        type=str,
        default=None,
        help='optional module init in the form "pkg.mod:func(arg1, arg2)" executed in each processor before tasks',
    )
    parser.add_argument("--num-of-workers", "-n", type=int, help="number of workers in cluster")
    parser.add_argument(
        "--worker-names",
        "-wn",
        type=str,
        help="worker names to replace default worker names (host names), separate by comma",
    )
    parser.add_argument(
        "--per-worker-capabilities",
        "-pwc",
        type=str,
        help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")',
    )
    parser.add_argument("--per-worker-task-queue-size", "-wtqs", type=int, help="specify per worker queue size")
    parser.add_argument(
        "--heartbeat-interval-seconds", "-hi", type=int, help="number of seconds to send heartbeat interval"
    )
    parser.add_argument(
        "--task-timeout-seconds",
        "-tts",
        type=int,
        help="number of seconds task treat as timeout and return an exception",
    )
    parser.add_argument("--garbage-collect-interval-seconds", "-gc", type=int, help="garbage collect interval seconds")
    parser.add_argument("--death-timeout-seconds", "-ds", type=int, help="death timeout seconds")
    parser.add_argument(
        "--trim-memory-threshold-bytes", "-tm", type=int, help="number of bytes threshold to enable libc to trim memory"
    )
    parser.add_argument("--event-loop", "-el", choices=EventLoopType.allowed_types(), help="select event loop type")
    parser.add_argument("--io-threads", "-it", type=int, help="specify number of io threads per worker")
    parser.add_argument(
        "--hard-processor-suspend",
        "-hps",
        action="store_true",
        help=(
            "When set, suspends worker processors using the SIGTSTP signal instead of a synchronization event, "
            "fully halting computation on suspended tasks. Note that this may cause some tasks to fail if they "
            "do not support being paused at the OS level (e.g. tasks requiring active network connections)."
        ),
    )
    parser.add_argument("--log-hub-address", "-la", type=str, help="address for Worker send logs")
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        help='specify where cluster log should logged to, it can be multiple paths, "/dev/stdout" is default for '
        "standard output, each worker will have its own log file with process id appended to the path",
    )
    parser.add_argument(
        "--logging-level",
        "-ll",
        type=str,
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        help="specify the logging level",
    )
    parser.add_argument(
        "--logging-config-file",
        "-lcf",
        type=str,
        help="use standard python the .conf file the specify python logging file configuration format, this will "
        "bypass --logging-paths and --logging-level at the same time, and this will not work on per worker logging",
    )
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=str,
        help="specify the object storage server address, e.g. tcp://localhost:2346. If not specified, use the address "
        "provided by the scheduler",
    )
    parser.add_argument("scheduler_address", nargs="?", type=str, help="scheduler address to connect to")
    parser.add_argument("--transport-type", type=str, help="the transport type to use, for example 'ymq' or 'zmq'")

    return parser.parse_args()


def main():
    args = get_args()

    cluster_config = load_config(ClusterConfig, args.config, args, section_name="cluster")

    register_event_loop(cluster_config.event_loop)

    worker_names = cluster_config.worker_names.names
    if not worker_names:
        worker_names = [f"{socket.gethostname().split('.')[0]}" for _ in range(cluster_config.num_of_workers)]

    if len(worker_names) != cluster_config.num_of_workers:
        raise ValueError(
            f"Number of worker names ({len(worker_names)}) must match the number of workers "
            f"({cluster_config.num_of_workers})."
        )

    cluster = Cluster(
        address=cluster_config.scheduler_address,
        storage_address=cluster_config.storage_address,
        preload=cluster_config.preload,
        worker_names=worker_names,
        per_worker_capabilities=cluster_config.per_worker_capabilities.capabilities,
        per_worker_task_queue_size=cluster_config.per_worker_task_queue_size,
        heartbeat_interval_seconds=cluster_config.heartbeat_interval_seconds,
        task_timeout_seconds=cluster_config.task_timeout_seconds,
        garbage_collect_interval_seconds=cluster_config.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=cluster_config.trim_memory_threshold_bytes,
        death_timeout_seconds=cluster_config.death_timeout_seconds,
        hard_processor_suspend=cluster_config.hard_processor_suspend,
        event_loop=cluster_config.event_loop,
        worker_io_threads=cluster_config.worker_io_threads,
        transport_type=args.transport_type,
        logging_paths=cluster_config.logging_paths,
        logging_level=cluster_config.logging_level,
        logging_config_file=cluster_config.logging_config_file,
    )
    cluster.run()
