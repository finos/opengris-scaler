import argparse

from aiohttp import web

from scaler.config.loader import load_config
from scaler.config.section.symphony_worker_adapter import SymphonyWorkerConfig
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.symphony.worker_adapter import SymphonyWorkerAdapter


def get_args():
    parser = argparse.ArgumentParser(
        "scaler Symphony worker adapter", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", "-c", type=str, default=None, help="Path to the TOML configuration file.")

    # Server configuration
    parser.add_argument("--adapter-web-host", type=str, help="host address for symphony worker adapter HTTP server")
    parser.add_argument("--adapter-web-port", "-p", type=int, help="port for symphony worker adapter HTTP server")

    # Symphony configuration
    parser.add_argument("--service-name", "-sn", type=str, help="symphony service name")
    parser.add_argument("--base-concurrency", "-n", type=int, help="base task concurrency")

    # Worker configuration
    parser.add_argument("--io-threads", "-it", help="specify number of io threads per worker")
    parser.add_argument(
        "--worker-capabilities",
        "-wc",
        type=str,
        help='comma-separated capabilities provided by the worker (e.g. "-wr linux,cpu=4")',
    )
    parser.add_argument("--worker-task-queue-size", "-wtqs", type=int, help="specify symphony worker queue size")
    parser.add_argument("--heartbeat-interval", "-hi", type=int, help="number of seconds to send heartbeat interval")
    parser.add_argument("--death-timeout-seconds", "-ds", type=int, help="death timeout seconds")
    parser.add_argument("--event-loop", "-el", choices=EventLoopType.allowed_types(), help="select event loop type")
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
        type=str,
        default=None,
        help="use standard python the .conf file the specify python logging file configuration format, this will "
        "bypass --logging-paths and --logging-level at the same time, and this will not work on per worker logging",
    )
    parser.add_argument(
        "--object-storage-address",
        "-osa",
        default=None,
        help="specify the object storage server address, e.g.: tcp://localhost:2346",
    )
    parser.add_argument("scheduler_address", nargs="?", type=str, help="scheduler address to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    symphony_config = load_config(SymphonyWorkerConfig, args.config, args, section_name="symphony_worker_adapter")
    register_event_loop(symphony_config.event_loop)

    setup_logger(symphony_config.logging_paths, symphony_config.logging_config_file, symphony_config.logging_level)

    symphony_worker_adapter = SymphonyWorkerAdapter(
        address=symphony_config.scheduler_address,
        object_storage_address=symphony_config.object_storage_address,
        capabilities=symphony_config.worker_capabilities.capabilities,
        task_queue_size=symphony_config.worker_task_queue_size,
        service_name=symphony_config.service_name,
        base_concurrency=symphony_config.base_concurrency,
        heartbeat_interval_seconds=symphony_config.heartbeat_interval,
        death_timeout_seconds=symphony_config.death_timeout_seconds,
        event_loop=symphony_config.event_loop,
        io_threads=symphony_config.io_threads,
        logging_paths=symphony_config.logging_paths,
        logging_level=symphony_config.logging_level,
        logging_config_file=symphony_config.logging_config_file,
    )

    app = symphony_worker_adapter.create_app()
    web.run_app(app, host=symphony_config.adapter_web_host, port=symphony_config.adapter_web_port)


if __name__ == "__main__":
    main()
