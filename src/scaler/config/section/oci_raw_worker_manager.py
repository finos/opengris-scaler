import dataclasses

from scaler.config.common.logging import LoggingConfig
from scaler.config.common.oci_container_instance import OCIContainerInstanceConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_manager import WorkerManagerConfig
from scaler.config.config_class import ConfigClass


@dataclasses.dataclass
class OCIRawWorkerManagerConfig(ConfigClass):
    worker_manager_config: WorkerManagerConfig
    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    container_instance_config: OCIContainerInstanceConfig = dataclasses.field(
        default_factory=OCIContainerInstanceConfig
    )

    python_requirements: str = dataclasses.field(
        default="tomli;pargraph;parfun;pandas",
        metadata=dict(help="Python requirements string passed to the container instance"),
    )
    python_version: str = dataclasses.field(
        default="3.12.11", metadata=dict(help="Python version for the container instance")
    )
    scaler_package: str = dataclasses.field(
        default="opengris-scaler[oci]",
        metadata=dict(
            env_var="SCALER_PACKAGE",
            help="Scaler package spec installed in the worker container (e.g. opengris-scaler[oci] or a wheel URL)",
        ),
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
