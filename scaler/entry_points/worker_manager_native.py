import argparse

from aiohttp import web

from scaler.config.loader import load_config
from scaler.config.section.worker_manager_native import WorkerManagerNativeConfig
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager.native import WorkerManagerNative


def get_args():
    parser = argparse.ArgumentParser(
        "scaler_native_worker_manager", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--config", "-c", type=str, default=None, help="Path to the TOML configuration file.")

    # Server configuration
    parser.add_argument("--manager-web-host", type=str, help="Host for the native worker manager HTTP server.")
    parser.add_argument("--manager-web-port", "-p", type=int, help="Port for the native worker manager HTTP server.")

    # Worker configuration
    parser.add_argument("--io-threads", type=int, help="number of io threads for zmq")
    parser.add_argument(
        "--per-worker-capabilities",
        "-pwc",
        type=str,
        help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")',
    )
    parser.add_argument("--worker-task-queue-size", "-wtqs", type=int, default=10, help="specify worker queue size")
    parser.add_argument(
        "--max-workers", "-mw", type=int, help="maximum number of workers that can be started, -1 means no limit"
    )
    parser.add_argument(
        "--heartbeat-interval", "-hi", type=int, help="number of seconds that worker agent send heartbeat to scheduler"
    )
    parser.add_argument(
        "--task-timeout-seconds", "-tt", type=int, help="default task timeout seconds, 0 means never timeout"
    )
    parser.add_argument(
        "--death-timeout-seconds",
        "-dt",
        type=int,
        help="number of seconds without scheduler contact before worker shuts down",
    )
    parser.add_argument(
        "--garbage-collect-interval-seconds", "-gc", type=int, help="number of seconds worker doing garbage collection"
    )
    parser.add_argument(
        "--trim-memory-threshold-bytes",
        "-tm",
        type=int,
        help="number of bytes threshold for worker process that trigger deep garbage collection",
    )
    parser.add_argument(
        "--hard-processor-suspend",
        "-hps",
        action="store_true",
        help="if true, suspended worker's processors will be actively suspended with a SIGTSTP signal",
    )
    parser.add_argument("--event-loop", "-e", choices=EventLoopType.allowed_types(), help="select event loop type")
    parser.add_argument(
        "--logging-paths",
        "-lp",
        nargs="*",
        type=str,
        help="specify where worker logs should be logged to, it can accept multiple files, default is /dev/stdout",
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
        "-lc",
        type=str,
        help="use standard python .conf file to specify python logging file configuration format",
    )
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        type=str,
        help="specify the object storage server address, e.g.: tcp://localhost:2346",
    )
    parser.add_argument(
        "scheduler_address",
        nargs="?",
        type=str,
        help="scheduler address to connect workers to, e.g.: `tcp://localhost:6378",
    )

    return parser.parse_args()


def main():
    args = get_args()
    worker_manager_native_config = load_config(
        WorkerManagerNativeConfig, args.config, args, section_name="worker_manager_native"
    )

    register_event_loop(worker_manager_native_config.event_loop)

    setup_logger(
        worker_manager_native_config.logging_paths,
        worker_manager_native_config.logging_config_file,
        worker_manager_native_config.logging_level,
    )

    worker_manager_native = WorkerManagerNative(
        address=worker_manager_native_config.scheduler_address,
        object_storage_address=worker_manager_native_config.object_storage_address,
        capabilities=worker_manager_native_config.per_worker_capabilities.capabilities,
        io_threads=worker_manager_native_config.io_threads,
        task_queue_size=worker_manager_native_config.worker_task_queue_size,
        max_workers=worker_manager_native_config.max_workers,
        heartbeat_interval_seconds=worker_manager_native_config.heartbeat_interval_seconds,
        task_timeout_seconds=worker_manager_native_config.task_timeout_seconds,
        death_timeout_seconds=worker_manager_native_config.death_timeout_seconds,
        garbage_collect_interval_seconds=worker_manager_native_config.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=worker_manager_native_config.trim_memory_threshold_bytes,
        hard_processor_suspend=worker_manager_native_config.hard_processor_suspend,
        event_loop=worker_manager_native_config.event_loop,
        logging_paths=worker_manager_native_config.logging_paths,
        logging_level=worker_manager_native_config.logging_level,
        logging_config_file=worker_manager_native_config.logging_config_file,
    )

    app = worker_manager_native.create_app()
    web.run_app(
        app, host=worker_manager_native_config.manager_web_host, port=worker_manager_native_config.manager_web_port
    )


if __name__ == "__main__":
    main()
