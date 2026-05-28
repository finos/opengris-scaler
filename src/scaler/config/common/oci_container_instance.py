import dataclasses
from typing import Optional

from scaler.config.config_class import ConfigClass
from scaler.config.types.oci_auth_type import OCIAuthType

DEFAULT_OCI_REGION = "us-ashburn-1"
DEFAULT_OCI_INSTANCE_SHAPE = "CI.Standard.E4.Flex"


@dataclasses.dataclass
class OCIContainerInstanceConfig(ConfigClass):
    # OCI authentication
    auth_type: OCIAuthType = dataclasses.field(
        default=OCIAuthType.config_file,
        metadata=dict(
            env_var="OCI_AUTH_TYPE",
            help="OCI authentication type: 'config_file' (uses ~/.oci/config) or 'instance_principal' (VM identity)",
        ),
    )
    oci_profile: str = dataclasses.field(
        default="DEFAULT",
        metadata=dict(
            env_var="OCI_CONFIG_PROFILE",
            help="OCI config file profile name (only used when auth-type is 'config_file')",
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
            env_var="OCI_SUBNET_ID", required=True, help="OCI Subnet OCID for container instance network interfaces"
        ),
    )

    # Container image
    container_image: Optional[str] = dataclasses.field(
        default=None,
        metadata=dict(
            env_var="OCI_CONTAINER_IMAGE",
            required=True,
            help="OCIR image URI for the container instance (e.g. <region>.ocir.io/<ns>/<repo>:latest)",
        ),
    )

    # Container instance shape
    instance_shape: str = dataclasses.field(
        default=DEFAULT_OCI_INSTANCE_SHAPE, metadata=dict(help="OCI Container Instance shape")
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
