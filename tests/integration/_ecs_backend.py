"""Process entry point that runs the SHIPPED ECS worker manager against a floci emulator.

Unlike ``_container_backend`` this defines no provisioner of its own: it configures and runs the real
``ECSWorkerManager`` (``scaler.worker_manager_adapter.aws_raw.ecs``), so the e2e exercises the exact code
path a production ECS deployment uses -- boto3 is merely pointed at floci through ``AWS_ENDPOINT_URL``.

The provisioner delivers the worker command through the task's ``COMMAND`` env var, so the task image only
needs an entrypoint that execs it (``ecs.Dockerfile``); the scaler wheel is prebaked, so
``ecs_python_requirements`` is empty and nothing is installed at task start. Workers reach the host
scheduler over the docker-bridge gateway (``worker_scheduler_address``) and, since the provisioner sets no
machine id, each task tags its work by container hostname (``socket.gethostname()``).
"""

from __future__ import annotations

# floci ignores Fargate resource requests and maps every awsvpc task onto the plain bridge, so the cluster
# name and subnet are arbitrary identifiers -- they only have to be internally consistent.
_ECS_CLUSTER = "scaler-it-ecs-cluster"
_ECS_TASK_DEFINITION = "scaler-it-ecs-taskdef"
_ECS_SUBNETS = ["subnet-e2e"]
_AWS_REGION = "us-east-1"


def run_ecs_worker_manager(
    scheduler_address: str,
    worker_scheduler_address: str,
    endpoint_url: str,
    ecs_task_image: str,
    ecs_task_cpu: int,
    max_task_concurrency: int,
    worker_manager_id: str = "wm-ecs-it",
) -> None:
    """Run the real ECS worker manager: it connects to the scheduler over loopback (``scheduler_address``)
    and, via boto3 aimed at ``endpoint_url`` (floci), provisions ECS task containers whose workers dial back
    over ``worker_scheduler_address`` (the docker-bridge gateway). ``ecs_task_cpu`` is the workers per task
    and the scale-up divisor; ``max_task_concurrency`` caps the pool at ``ceil(mtc / ecs_task_cpu)`` tasks."""
    import os

    # Set on the child only (not the parent) so the shipped provisioner's boto3 clients hit floci.
    os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", _AWS_REGION)

    from scaler.config.common.worker import WorkerConfig
    from scaler.config.common.worker_manager import WorkerManagerConfig
    from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
    from scaler.config.types.address import AddressConfig
    from scaler.config.types.worker import WorkerCapabilities
    from scaler.utility.logging.utility import setup_logger
    from scaler.worker_manager_adapter.aws_raw.ecs import ECSWorkerManager

    setup_logger()
    config = ECSWorkerManagerConfig(
        worker_manager_config=WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string(scheduler_address),
            worker_scheduler_address=AddressConfig.from_string(worker_scheduler_address),
            worker_manager_id=worker_manager_id,
            max_task_concurrency=max_task_concurrency,
        ),
        # Prefetch one task per worker so the scheduler queue stays non-empty while tasks are in flight,
        # keeping the scaling signal honest (workers otherwise hoard the burst and the pool tears down).
        worker_config=WorkerConfig(per_worker_capabilities=WorkerCapabilities({}), per_worker_task_queue_size=1),
        aws_region=_AWS_REGION,
        ecs_subnets=list(_ECS_SUBNETS),
        ecs_task_image=ecs_task_image,
        ecs_python_requirements="",  # scaler wheel is prebaked into the image; nothing to install at task start
        ecs_task_cpu=ecs_task_cpu,
        # Fargate only accepts specific cpu/memory pairs; 2 GB per vCPU is the minimum valid memory for every
        # cpu size, so the provisioner's register_task_definition is always accepted (floci enforces this).
        ecs_task_memory=ecs_task_cpu * 2,
        ecs_cluster=_ECS_CLUSTER,
        ecs_task_definition=_ECS_TASK_DEFINITION,
    )
    ECSWorkerManager(config).run()
