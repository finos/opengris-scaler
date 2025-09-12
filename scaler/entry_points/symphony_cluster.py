import argparse
import logging
import os
import signal
import socket

from scaler.config.config import SymphonyWorkerConfig
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker.symphony.worker import SymphonyWorker


def get_args():
    parser = argparse.ArgumentParser(
        "standalone symphony cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML configuration file.")
    parser.add_argument("--base-concurrency", "-n", type=int, help="base task concurrency")
    parser.add_argument(
        "--worker-name", "-w", type=str, default=None, help="worker name, if not specified, it will be hostname"
    )
    parser.add_argument(
        "--worker-capabilities",
        "-wc",
        type=str,
        default="",
        help='comma-separated capabilities provided by the worker (e.g. "-wr linux,cpu=4")',
    )
    parser.add_argument("--worker-task-queue-size", "-wtqs", type=int, help="specify symphony worker queue size")
    parser.add_argument("--heartbeat-interval", "-hi", type=int, help="number of seconds to send heartbeat interval")
    parser.add_argument("--death-timeout-seconds", "-ds", type=int, help="death timeout seconds")
    parser.add_argument("--event-loop", "-el", choices=EventLoopType.allowed_types(), help="select event loop type")
    parser.add_argument("--io-threads", "-it", help="specify number of io threads per worker")
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
    parser.add_argument("scheduler_address", nargs="?", type=str, help="scheduler address to connect to")
    parser.add_argument("service_name", nargs="?", type=str, help="symphony service name")
    return parser.parse_args()


def main():
    args = get_args()
    symphony_config = SymphonyWorkerConfig.get_config(args.config, args)
    register_event_loop(symphony_config.event_loop)

    worker_name = symphony_config.worker_name
    if worker_name is None:
        worker_name = f"{socket.gethostname().split('.')[0]}"

    setup_logger(symphony_config.logging_paths, symphony_config.logging_config_file, symphony_config.logging_level)

    worker = SymphonyWorker(
        address=symphony_config.scheduler_address,
        name=worker_name,
        capabilities=symphony_config.worker_capabilities.capabilities,
        task_queue_size=symphony_config.worker_task_queue_size,
        service_name=symphony_config.service_name,
        base_concurrency=symphony_config.base_concurrency,
        heartbeat_interval_seconds=symphony_config.heartbeat_interval_seconds,
        death_timeout_seconds=symphony_config.death_timeout_seconds,
        event_loop=symphony_config.event_loop,
        io_threads=symphony_config.io_threads,
    )

    def destroy(*_args):
        assert _args is not None
        logging.info(f"{SymphonyWorker.__class__.__name__}: shutting down Symphony worker[{worker.pid}]")
        os.kill(worker.pid, signal.SIGINT)

    signal.signal(signal.SIGINT, destroy)
    signal.signal(signal.SIGTERM, destroy)

    worker.start()
    logging.info("Symphony worker started")

    worker.join()
    logging.info("Symphony worker stopped")
