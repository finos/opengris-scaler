import dataclasses
from typing import ClassVar, Optional

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.oci_container_instance import OCIContainerInstanceConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass

DEFAULT_OCI_OBJECT_STORAGE_PREFIX = "scaler-tasks"
DEFAULT_OCI_HPC_MAX_CONCURRENT_JOBS = 100
DEFAULT_OCI_HPC_JOB_TIMEOUT_SECONDS = 3600


@dataclasses.dataclass
class OCIHPCWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "oci_hpc"

    worker_manager_config: WorkerManagerConfig
    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    container_instance_config: OCIContainerInstanceConfig = dataclasses.field(
        default_factory=OCIContainerInstanceConfig
    )

    # Object Storage
    object_storage_namespace: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_OBJECT_STORAGE_NAMESPACE", required=True, help="OCI Object Storage tenancy namespace"
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
        metadata=dict(env_var="OCI_OBJECT_STORAGE_PREFIX", help="Object key prefix for task inputs and results"),
    )

    # Container instance sizing
    instance_ocpus: float = dataclasses.field(default=1.0, metadata=dict(help="Number of OCPUs per container instance"))
    instance_memory_gb: float = dataclasses.field(
        default=6.0, metadata=dict(help="Memory in GB per container instance")
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

    def __post_init__(self) -> None:
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
