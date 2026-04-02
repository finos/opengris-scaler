import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType

DEFAULT_OCI_REGION = "us-ashburn-1"
DEFAULT_OCI_OBJECT_STORAGE_PREFIX = "scaler-tasks"
DEFAULT_OCI_INSTANCE_SHAPE = "CI.Standard.E4.Flex"
DEFAULT_OCI_HPC_MAX_CONCURRENT_JOBS = 100
DEFAULT_OCI_HPC_JOB_TIMEOUT_SECONDS = 3600


@dataclasses.dataclass
class OCIHPCWorkerAdapterConfig(ConfigClass):
    worker_adapter_config: WorkerAdapterConfig
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)

    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )
    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-wit", help="number of IO threads for the worker"),
    )

    # OCI authentication
    oci_auth_type: str = dataclasses.field(
        default="config_file",
        metadata=dict(
            env_var="OCI_AUTH_TYPE",
            choices=["config_file", "instance_principal"],
            help="OCI authentication type: 'config_file' (uses ~/.oci/config) or 'instance_principal' (VM identity)",
        ),
    )
    oci_config_profile: str = dataclasses.field(
        default="DEFAULT",
        metadata=dict(
            env_var="OCI_CONFIG_PROFILE",
            help="OCI config file profile name (only used when oci-auth-type is 'config_file')",
        ),
    )

    # OCI resource identifiers
    oci_region: str = dataclasses.field(
        default=DEFAULT_OCI_REGION,
        metadata=dict(env_var="OCI_REGION", help="OCI region identifier (e.g. us-ashburn-1)"),
    )
    compartment_id: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_COMPARTMENT_ID",
            required=True,
            help="OCI Compartment OCID where container instances are launched",
        ),
    )
    availability_domain: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_AVAILABILITY_DOMAIN",
            required=True,
            help="OCI Availability Domain for container instances (e.g. AD-1 or Uocm:PHX-AD-1)",
        ),
    )
    subnet_id: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_SUBNET_ID",
            required=True,
            help="OCI Subnet OCID for container instance network interfaces",
        ),
    )

    # Container image
    container_image: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_CONTAINER_IMAGE",
            required=True,
            help="OCIR image URI for the job runner container (e.g. <region>.ocir.io/<ns>/<repo>:latest)",
        ),
    )

    # Object Storage
    object_storage_namespace: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_OBJECT_STORAGE_NAMESPACE",
            required=True,
            help="OCI Object Storage tenancy namespace",
        ),
    )
    object_storage_bucket: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_OBJECT_STORAGE_BUCKET",
            required=True,
            help="OCI Object Storage bucket name for task inputs and results",
        ),
    )
    object_storage_prefix: str = dataclasses.field(
        default=DEFAULT_OCI_OBJECT_STORAGE_PREFIX,
        metadata=dict(
            env_var="OCI_OBJECT_STORAGE_PREFIX",
            help="Object key prefix for task inputs and results",
        ),
    )

    # Container instance sizing
    instance_shape: str = dataclasses.field(
        default=DEFAULT_OCI_INSTANCE_SHAPE,
        metadata=dict(help="OCI Container Instance shape"),
    )
    instance_ocpus: float = dataclasses.field(
        default=1.0,
        metadata=dict(help="Number of OCPUs per container instance"),
    )
    instance_memory_gb: float = dataclasses.field(
        default=6.0,
        metadata=dict(help="Memory in GB per container instance"),
    )

    # Concurrency and timeouts
    base_concurrency: int = dataclasses.field(
        default=DEFAULT_OCI_HPC_MAX_CONCURRENT_JOBS,
        metadata=dict(short="-bc", help="maximum number of concurrently running container instances"),
    )
    job_timeout_seconds: int = dataclasses.field(
        default=DEFAULT_OCI_HPC_JOB_TIMEOUT_SECONDS,
        metadata=dict(help="maximum runtime in seconds for a single container instance task"),
    )
    heartbeat_interval_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        metadata=dict(short="-his", help="heartbeat interval in seconds"),
    )
    death_timeout_seconds: int = dataclasses.field(
        default=defaults.DEFAULT_WORKER_DEATH_TIMEOUT,
        metadata=dict(short="-dts", help="death timeout in seconds"),
    )
    task_queue_size: int = dataclasses.field(
        default=defaults.DEFAULT_PER_WORKER_QUEUE_SIZE,
        metadata=dict(help="size of the internal task queue reported to the scheduler"),
    )

    def __post_init__(self) -> None:
        if not self.compartment_id:
            raise ValueError("compartment_id cannot be empty.")
        if not self.availability_domain:
            raise ValueError("availability_domain cannot be empty.")
        if not self.subnet_id:
            raise ValueError("subnet_id cannot be empty.")
        if not self.container_image:
            raise ValueError("container_image cannot be empty.")
        if not self.object_storage_namespace:
            raise ValueError("object_storage_namespace cannot be empty.")
        if not self.object_storage_bucket:
            raise ValueError("object_storage_bucket cannot be empty.")
        if self.instance_ocpus <= 0:
            raise ValueError("instance_ocpus must be a positive number.")
        if self.instance_memory_gb <= 0:
            raise ValueError("instance_memory_gb must be a positive number.")
        if self.base_concurrency <= 0:
            raise ValueError("base_concurrency must be a positive integer.")
        if self.job_timeout_seconds <= 0:
            raise ValueError("job_timeout_seconds must be a positive integer.")
        if self.heartbeat_interval_seconds <= 0:
            raise ValueError("heartbeat_interval_seconds must be a positive integer.")
        if self.death_timeout_seconds <= 0:
            raise ValueError("death_timeout_seconds must be a positive integer.")
        if self.task_queue_size <= 0:
            raise ValueError("task_queue_size must be a positive integer.")
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
