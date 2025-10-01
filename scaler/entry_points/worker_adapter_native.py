import argparse

from aiohttp import web

from scaler.entry_points.cluster import parse_capabilities
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
from scaler.utility.logging.utility import setup_logger
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker_adapter.native import NativeWorkerAdapter


def get_args():
    parser = argparse.ArgumentParser(
        "scaler native worker adapter", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Server configuration
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="host address for the native worker adapter HTTP server"
    )
    parser.add_argument("--port", "-p", type=int, help="port for the native worker adapter HTTP server")

    # Worker configuration
    parser.add_argument("--io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
    parser.add_argument(
        "--per-worker-capabilities",
        "-pwc",
        type=parse_capabilities,
        default="",
        help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")',
    )
    parser.add_argument("--worker-task-queue-size", "-wtqs", type=int, default=10, help="specify worker queue size")
    parser.add_argument(
        "--max-workers",
        "-mw",
        type=int,
        default=DEFAULT_NUMBER_OF_WORKER,
        help="maximum number of workers that can be started, -1 means no limit",
    )
    parser.add_argument(
        "--heartbeat-interval",
        "-hi",
        type=int,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="number of seconds that worker agent send heartbeat to scheduler",
    )
    parser.add_argument(
        "--task-timeout-seconds",
        "-tt",
        type=int,
        default=DEFAULT_TASK_TIMEOUT_SECONDS,
        help="default task timeout seconds, 0 means never timeout",
    )
    parser.add_argument(
        "--death-timeout-seconds",
        "-dt",
        type=int,
        default=DEFAULT_WORKER_DEATH_TIMEOUT,
        help="number of seconds without scheduler contact before worker shuts down",
    )
    parser.add_argument(
        "--garbage-collect-interval-seconds",
        "-gc",
        type=int,
        default=DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        help="number of seconds worker doing garbage collection",
    )
    parser.add_argument(
        "--trim-memory-threshold-bytes",
        "-tm",
        type=int,
        default=DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        help="number of bytes threshold for worker process that trigger deep garbage collection",
    )
    parser.add_argument(
        "--hard-processor-suspend",
        "-hps",
        action="store_true",
        default=DEFAULT_HARD_PROCESSOR_SUSPEND,
        help="if true, suspended worker's processors will be actively suspended with a SIGTSTP signal",
    )
    parser.add_argument(
        "--event-loop", "-e", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
    )
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        default=("/dev/stdout",),
        help="specify where worker logs should be logged to, it can accept multiple files, default is /dev/stdout",
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
        help="use standard python .conf file to specify python logging file configuration format",
    )
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=ObjectStorageConfig.from_string,
        default=None,
        help="specify the object storage server address, e.g.: tcp://localhost:2346",
    )
    parser.add_argument(
        "scheduler_address",
        type=ZMQConfig.from_string,
        help="scheduler address to connect workers to, e.g.: `tcp://localhost:6378`",
    )

    return parser.parse_args()


def main():
    args = get_args()
    register_event_loop(args.event_loop)

    setup_logger(args.logging_paths, args.logging_config_file, args.logging_level)

    native_worker_adapter = NativeWorkerAdapter(
        address=args.scheduler_address,
        storage_address=args.object_storage_address,
        capabilities=args.per_worker_capabilities,
        io_threads=args.io_threads,
        task_queue_size=args.worker_task_queue_size,
        max_workers=args.max_workers,
        heartbeat_interval_seconds=args.heartbeat_interval,
        task_timeout_seconds=args.task_timeout_seconds,
        death_timeout_seconds=args.death_timeout_seconds,
        garbage_collect_interval_seconds=args.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=args.trim_memory_threshold_bytes,
        hard_processor_suspend=args.hard_processor_suspend,
        event_loop=args.event_loop,
        logging_paths=args.logging_paths,
        logging_level=args.logging_level,
        logging_config_file=args.logging_config_file,
    )

    app = native_worker_adapter.create_app()
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
