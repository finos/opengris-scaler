import argparse
import os

from aiohttp import web

from scaler.config import defaults
from scaler.config.loader import load_config
from scaler.config.section.ecs_worker_adapter import ECSWorkerAdapterConfig
from scaler.utility.event_loop import EventLoopType, register_event_loop
from scaler.utility.logging.utility import setup_logger
from scaler.worker_adapter.ecs import ECSWorkerAdapter


def get_args():
    parser = argparse.ArgumentParser(
        "scaler ECS worker adapter", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("--config", "-c", type=str, default=None, help="Path to the TOML configuration file.")

    # Server configuration (match dataclass field names)
    parser.add_argument("--adapter-web-host", type=str, help="host address for the ecs worker adapter HTTP server")
    parser.add_argument("--adapter-web-port", "-p", type=int, help="port for the ecs worker adapter HTTP server")

    # AWS / ECS configuration
    parser.add_argument(
        "--aws-access-key-id",
        type=str,
        default=os.environ.get("AWS_ACCESS_KEY_ID"),
        help="AWS access key id (or set AWS_ACCESS_KEY_ID env)",
    )
    parser.add_argument(
        "--aws-secret-access-key",
        type=str,
        default=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        help="AWS secret access key (or set AWS_SECRET_ACCESS_KEY env)",
    )
    parser.add_argument("--aws-region", type=str, default="us-east-1", help="AWS region for ECS cluster")
    parser.add_argument("--ecs-cluster", type=str, help="ECS cluster name")
    parser.add_argument("--ecs-task-image", type=str, help="Container image used for ECS tasks")
    parser.add_argument("--ecs-python-requirements", type=str, help="Python requirements string passed to the ECS task")
    parser.add_argument("--ecs-python-version", type=str, help="Python version for ECS task")
    parser.add_argument("--ecs-task-definition", type=str, help="ECS task definition")
    parser.add_argument("--ecs-task-cpu", type=int, help="Number of vCPUs for task (used to derive worker count)")
    parser.add_argument("--ecs-task-memory", type=int, help="Task memory in GB for Fargate")
    parser.add_argument("--ecs-subnets", type=str, help="Comma-separated list of AWS subnet IDs for ECS tasks")

    # Worker configuration
    parser.add_argument(
        "--io-threads", "-it", type=int, default=defaults.DEFAULT_IO_THREADS, help="number of io threads for zmq"
    )
    parser.add_argument(
        "--per-worker-capabilities",
        "-pwc",
        type=str,
        help='comma-separated capabilities provided by the workers (e.g. "-pwc linux,cpu=4")',
    )
    parser.add_argument("--per-worker-task-queue-size", "-wtqs", type=int, help="specify worker queue size")
    parser.add_argument(
        "--max-instances",
        "-mi",
        type=int,
        help="maximum number of ECS task instances that can be started, required to avoid unexpected surprise bills, "
        "-1 means no limit",
    )
    parser.add_argument(
        "--heartbeat-interval-seconds",
        "-hi",
        type=int,
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        help="number of seconds that worker agent send heartbeat to scheduler",
    )
    parser.add_argument(
        "--task-timeout-seconds",
        "-tt",
        type=int,
        default=defaults.DEFAULT_TASK_TIMEOUT_SECONDS,
        help="default task timeout seconds, 0 means never timeout",
    )
    parser.add_argument(
        "--death-timeout-seconds",
        "-dt",
        type=int,
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT,
        help="number of seconds without scheduler contact before worker shuts down",
    )
    parser.add_argument(
        "--garbage-collect-interval-seconds",
        "-gc",
        type=int,
        default=defaults.DEFAULT_GARBAGE_COLLECT_INTERVAL_SECONDS,
        help="number of seconds worker doing garbage collection",
    )
    parser.add_argument(
        "--trim-memory-threshold-bytes",
        "-tm",
        type=int,
        default=defaults.DEFAULT_TRIM_MEMORY_THRESHOLD_BYTES,
        help="number of bytes threshold for worker process that trigger deep garbage collection",
    )
    parser.add_argument(
        "--hard-processor-suspend",
        "-hps",
        action="store_true",
        default=defaults.DEFAULT_HARD_PROCESSOR_SUSPEND,
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
        type=str,
        default=None,
        help="specify the object storage server address, e.g.: tcp://localhost:2346",
    )
    parser.add_argument("scheduler_address", nargs="?", type=str, help="scheduler address to connect workers to")

    return parser.parse_args()


def main():
    args = get_args()

    ecs_config = load_config(ECSWorkerAdapterConfig, args.config, args, section_name="ecs_worker_adapter")

    # If ecs_subnets was provided as a comma-separated string on CLI, convert to list
    if isinstance(ecs_config.ecs_subnets, str):
        ecs_config.ecs_subnets = [s for s in ecs_config.ecs_subnets.split(",") if s]

    register_event_loop(ecs_config.event_loop)

    setup_logger(ecs_config.logging_paths, ecs_config.logging_config_file, ecs_config.logging_level)

    ecs_worker_adapter = ECSWorkerAdapter(
        address=ecs_config.scheduler_address,
        object_storage_address=ecs_config.object_storage_address,
        capabilities=ecs_config.per_worker_capabilities.capabilities,
        io_threads=ecs_config.io_threads,
        per_worker_task_queue_size=ecs_config.per_worker_task_queue_size,
        max_instances=ecs_config.max_instances,
        heartbeat_interval_seconds=ecs_config.heartbeat_interval_seconds,
        task_timeout_seconds=ecs_config.task_timeout_seconds,
        death_timeout_seconds=ecs_config.death_timeout_seconds,
        garbage_collect_interval_seconds=ecs_config.garbage_collect_interval_seconds,
        trim_memory_threshold_bytes=ecs_config.trim_memory_threshold_bytes,
        hard_processor_suspend=ecs_config.hard_processor_suspend,
        event_loop=ecs_config.event_loop,
        aws_access_key_id=ecs_config.aws_access_key_id,
        aws_secret_access_key=ecs_config.aws_secret_access_key,
        aws_region=ecs_config.aws_region,
        ecs_subnets=ecs_config.ecs_subnets,
        ecs_cluster=ecs_config.ecs_cluster,
        ecs_task_image=ecs_config.ecs_task_image,
        ecs_python_requirements=ecs_config.ecs_python_requirements,
        ecs_python_version=ecs_config.ecs_python_version,
        ecs_task_definition=ecs_config.ecs_task_definition,
        ecs_task_cpu=ecs_config.ecs_task_cpu,
        ecs_task_memory=ecs_config.ecs_task_memory,
    )

    app = ecs_worker_adapter.create_app()
    web.run_app(app, host=ecs_config.adapter_web_host, port=ecs_config.adapter_web_port)


if __name__ == "__main__":
    main()
