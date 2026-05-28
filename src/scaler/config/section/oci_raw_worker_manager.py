import dataclasses
from typing import ClassVar

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.oci_container_instance import OCIContainerInstanceConfig
from scaler.config.common.python_worker_environment import PythonWorkerEnvironmentConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class OCIRawWorkerManagerConfig(ConfigClass):
    _tag: ClassVar[str] = "oci_raw_container_instance"
    worker_manager_config: WorkerManagerConfig
    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    container_instance_config: OCIContainerInstanceConfig = dataclasses.field(
        default_factory=OCIContainerInstanceConfig
    )

    python_worker_environment: PythonWorkerEnvironmentConfig = dataclasses.field(
        default_factory=PythonWorkerEnvironmentConfig
    )

    # Container instance sizing
    instance_ocpus: float = dataclasses.field(
        default=4.0, metadata=dict(help="Number of OCPUs per container instance (also determines worker count)")
    )
    instance_memory_gb: float = dataclasses.field(
        default=30.0, metadata=dict(help="Memory in GB per container instance")
    )

    def __post_init__(self) -> None:
        if self.instance_ocpus <= 0:
            raise ValueError("instance_ocpus must be a positive number.")
        if self.instance_memory_gb <= 0:
            raise ValueError("instance_memory_gb must be a positive number.")
        if not self.python_worker_environment.requirements_txt:
            raise ValueError("--requirements-txt must be provided")
        if not self.python_worker_environment.python_version:
            raise ValueError("--python-version must be provided")
