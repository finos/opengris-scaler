import argparse

from aiohttp import web

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
from scaler.utility.event_loop import EventLoopType
from scaler.config import ObjectStorageConfig, ScalerConfig
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker_adapter.native import NativeWorkerAdapter


def get_args():
    parser = argparse.ArgumentParser(
        "scaler native worker adapter", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--config", type=str, default=None, help="Path to the TOML configuration file.")

    # Server configuration
    parser.add_argument(
        "--adapter_web_host",
        type=str,
        default="localhost",
        help="host address for the native worker adapter HTTP server",
    )
    parser.add_argument("--adapter_web_port", "-awp", type=int, help="port for the native worker adapter HTTP server")

    # Worker configuration
    parser.add_argument("--io-threads", type=int, default=DEFAULT_IO_THREADS, help="number of io threads for zmq")
    parser.add_argument(
        "--per-worker-capabilities-str",
        "-pwc",
        type=str,
        default="",
        help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")',
    )
    parser.add_argument("--worker-task-queue-size", "-wtqs", type=int, default=10, help="specify worker queue size")
    parser.add_argument(
        "--num-of-workers",
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

    scaler_config = ScalerConfig.get_config(args.config, args)

    native_worker_adapter = NativeWorkerAdapter(
        address=scaler_config.scheduler.address,
        storage_address=scaler_config.object_storage,
        capabilities=scaler_config.cluster.per_worker_capabilities,
        io_threads=scaler_config.worker.io_threads,
        task_queue_size=scaler_config.worker.per_worker_task_queue_size,
        max_workers=scaler_config.cluster.num_of_workers,
        heartbeat_interval_seconds=scaler_config.worker.heartbeat_interval_seconds,
        task_timeout_seconds=scaler_config.worker.task_timeout_seconds,
        death_timeout_seconds=scaler_config.worker.death_timeout_seconds,
        garbage_collect_interval_seconds=scaler_config.worker.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=scaler_config.worker.trim_memory_threshold_bytes,
        hard_processor_suspend=scaler_config.worker.hard_processor_suspend,
        event_loop=scaler_config.scheduler.event_loop,
        logging_paths=scaler_config.logging.paths,
        logging_level=scaler_config.logging.level,
        logging_config_file=scaler_config.logging.config_file,
    )

    app = native_worker_adapter.create_app()
    web.run_app(
        app, host=scaler_config.native_adapter.adapter_web_host, port=scaler_config.native_adapter.adapter_web_port
    )


if __name__ == "__main__":
    main()
