import dataclasses
from typing import Optional

from scaler.config import defaults
from scaler.config.common.logging import LoggingConfig
from scaler.config.common.worker import WorkerConfig
from scaler.config.common.worker_adapter import WorkerAdapterConfig
from scaler.config.config_class import ConfigClass
from scaler.utility.event_loop import EventLoopType


@dataclasses.dataclass
class OCIRawWorkerAdapterConfig(ConfigClass):
    worker_adapter_config: WorkerAdapterConfig
    worker_config: WorkerConfig = dataclasses.field(default_factory=WorkerConfig)
    logging_config: LoggingConfig = dataclasses.field(default_factory=LoggingConfig)
    event_loop: str = dataclasses.field(
        default="builtin",
        metadata=dict(short="-el", choices=EventLoopType.allowed_types(), help="select the event loop type"),
    )

    worker_io_threads: int = dataclasses.field(
        default=defaults.DEFAULT_IO_THREADS,
        metadata=dict(short="-wit", help="set the number of io threads for io backend per worker"),
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
        default="us-ashburn-1", metadata=dict(env_var="OCI_REGION", help="OCI region identifier (e.g. us-ashburn-1)")
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
            env_var="OCI_SUBNET_ID", required=True, help="OCI Subnet OCID for container instance network interfaces"
        ),
    )

    # Container image
    container_image: str = dataclasses.field(
        default="",
        metadata=dict(
            env_var="OCI_CONTAINER_IMAGE",
            required=True,
            help="OCIR image URI used for container instances (e.g. <region>.ocir.io/<ns>/<repo>:latest)",
        ),
    )
    oci_python_requirements: str = dataclasses.field(
        default="tomli;pargraph;parfun;pandas",
        metadata=dict(help="Python requirements string passed to the container instance"),
    )
    oci_python_version: str = dataclasses.field(
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
    instance_shape: str = dataclasses.field(
        default="CI.Standard.E4.Flex", metadata=dict(help="OCI Container Instance shape")
    )
    instance_ocpus: float = dataclasses.field(
        default=4.0, metadata=dict(help="Number of OCPUs per container instance (also determines worker count)")
    )
    instance_memory_gb: float = dataclasses.field(
        default=30.0, metadata=dict(help="Memory in GB per container instance")
    )

    def __post_init__(self):
        if self.instance_ocpus <= 0:
            raise ValueError("instance_ocpus must be a positive number.")
        if self.instance_memory_gb <= 0:
            raise ValueError("instance_memory_gb must be a positive number.")
        if not self.compartment_id:
            raise ValueError("compartment_id cannot be empty.")
        if not self.availability_domain:
            raise ValueError("availability_domain cannot be empty.")
        if not self.subnet_id:
            raise ValueError("subnet_id cannot be empty.")
        if not self.container_image:
            raise ValueError("container_image cannot be empty.")
        if self.worker_io_threads <= 0:
            raise ValueError("worker_io_threads must be a positive integer.")
