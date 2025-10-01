import argparse

from aiohttp import web

from scaler.entry_points.cluster import parse_capabilities
from scaler.io.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_NUMBER_OF_WORKER,
    DEFAULT_PER_WORKER_QUEUE_SIZE,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.utility.object_storage_config import ObjectStorageConfig
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker_adapter.symphony.worker_adapter import SymphonyWorkerAdapter


def get_args():
    parser = argparse.ArgumentParser(
        "scaler Symphony worker adapter", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Server configuration
    parser.add_argument(
        "--host", type=str, default="127.0.0.1", help="host address for the native worker adapter HTTP server"
    )
    parser.add_argument("--port", "-p", type=int, required=True, help="port for the native worker adapter HTTP server")

    # Symphony configuration
    parser.add_argument("--service-name", "-sn", type=str, required=True, help="symphony service name")
    parser.add_argument(
        "--base-concurrency", "-n", type=int, default=DEFAULT_NUMBER_OF_WORKER, help="base task concurrency"
    )

    # Worker configuration
    parser.add_argument(
        "--io-threads", "-it", default=DEFAULT_IO_THREADS, help="specify number of io threads per worker"
    )
    parser.add_argument(
        "--worker-capabilities",
        "-wc",
        type=parse_capabilities,
        default="",
        help='comma-separated capabilities provided by the worker (e.g. "-wr linux,cpu=4")',
    )
    parser.add_argument(
        "--worker-task-queue-size",
        "-wtqs",
        type=int,
        default=DEFAULT_PER_WORKER_QUEUE_SIZE,
        help="specify symphony worker queue size",
    )
    parser.add_argument(
        "--heartbeat-interval",
        "-hi",
        type=int,
        default=DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="number of seconds to send heartbeat interval",
    )
    parser.add_argument(
        "--death-timeout-seconds", "-ds", type=int, default=DEFAULT_WORKER_DEATH_TIMEOUT, help="death timeout seconds"
    )
    parser.add_argument(
        "--event-loop", "-el", default="builtin", choices=EventLoopType.allowed_types(), help="select event loop type"
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
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=ObjectStorageConfig.from_string,
        default=None,
        help="specify the object storage server address, e.g.: tcp://localhost:2346",
    )
    parser.add_argument("address", type=ZMQConfig.from_string, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    register_event_loop(args.event_loop)

    setup_logger(args.logging_paths, args.logging_config_file, args.logging_level)

    symphony_worker_adapter = SymphonyWorkerAdapter(
        address=args.address,
        storage_address=args.object_storage_address,
        capabilities=args.worker_capabilities,
        task_queue_size=args.worker_task_queue_size,
        service_name=args.service_name,
        base_concurrency=args.base_concurrency,
        heartbeat_interval_seconds=args.heartbeat_interval,
        death_timeout_seconds=args.death_timeout_seconds,
        event_loop=args.event_loop,
        io_threads=args.io_threads,
        logging_paths=args.logging_paths,
        logging_level=args.logging_level,
        logging_config_file=args.logging_config_file,
    )

    app = symphony_worker_adapter.create_app()
    web.run_app(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
