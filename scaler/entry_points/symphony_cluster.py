import argparse
import logging
import os
import signal
import socket

from scaler.io.config import (
    DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
    DEFAULT_IO_THREADS,
    DEFAULT_NUMBER_OF_WORKER,
    DEFAULT_WORKER_DEATH_TIMEOUT,
)
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.utility.zmq_config import ZMQConfig
from scaler.worker.symphony.worker import SymphonyWorker


def get_args():
    parser = argparse.ArgumentParser(
        "standalone symphony cluster", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--base-concurrency", "-n", type=int, default=DEFAULT_NUMBER_OF_WORKER, help="base task concurrency"
    )
    parser.add_argument(
        "--worker-name", "-w", type=str, default=None, help="worker name, if not specified, it will be hostname"
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
        "--io-threads", "-it", default=DEFAULT_IO_THREADS, help="specify number of io threads per worker"
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
    parser.add_argument("service_name", type=str, help="symphony service name")
    return parser.parse_args()


def main():
    args = get_args()
    register_event_loop(args.event_loop)

    if args.worker_name is None:
        args.worker_name = f"{socket.gethostname().split('.')[0]}"

    setup_logger(args.logging_paths, args.logging_config_file, args.logging_level)

    worker = SymphonyWorker(
        address=args.address,
        name=args.worker_name,
        service_name=args.service_name,
        base_concurrency=args.base_concurrency,
        heartbeat_interval_seconds=args.heartbeat_interval,
        death_timeout_seconds=args.death_timeout_seconds,
        event_loop=args.event_loop,
        io_threads=args.io_threads,
    )

    def destroy(*args):
        assert args is not None
        logging.info(f"{SymphonyWorker.__class__.__name__}: shutting down Symphony worker[{worker.pid}]")
        os.kill(worker.pid, signal.SIGINT)

    signal.signal(signal.SIGINT, destroy)
    signal.signal(signal.SIGTERM, destroy)

    worker.start()
    logging.info("Symphony worker started")

    worker.join()
    logging.info("Symphony worker stopped")
