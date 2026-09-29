"""
Provision and clean up the OCI resources the OCI Raw worker manager needs:
    - a VCN with a public subnet, internet gateway, route table, and security list
    - a private OCIR repository holding the worker image built from Dockerfile.container_instance
    - an IAM policy that lets container instances in the compartment pull from that repository

Every resource is named after --prefix, so provision is safe to rerun and cleanup finds what provision made.
"""

import argparse
import logging
import subprocess
from pathlib import Path
from typing import Any, Optional

import oci

logger = logging.getLogger(__name__)

DEFAULT_PREFIX = "scaler"
DEFAULT_REGION = "us-ashburn-1"
DEFAULT_VCN_CIDR = "10.0.0.0/16"
DEFAULT_SUBNET_CIDR = "10.0.0.0/24"
ANYWHERE_CIDR = "0.0.0.0/0"
ALL_PROTOCOLS = "all"
AVAILABLE_STATE = "AVAILABLE"

# CI.Standard.E4.Flex, the default container instance shape, is x86_64
IMAGE_PLATFORM = "linux/amd64"
DOCKERFILE_PATH = Path(__file__).parent / "Dockerfile.container_instance"
# The Dockerfile copies paths relative to the directory holding the scaler package
DOCKER_BUILD_CONTEXT = Path(__file__).parents[4]


class OCIRawProvisioner:
    def __init__(self, compartment_id: str, region: str, prefix: str, profile: str) -> None:
        self._compartment_id = compartment_id
        self._region = region
        self._prefix = prefix

        config = oci.config.from_file(profile_name=profile)
        config["region"] = region
        self._tenancy_id = config["tenancy"]

        self._network = oci.core.VirtualNetworkClient(config)
        self._network_ops = oci.core.VirtualNetworkClientCompositeOperations(self._network)
        self._artifacts = oci.artifacts.ArtifactsClient(config)
        self._artifacts_ops = oci.artifacts.ArtifactsClientCompositeOperations(self._artifacts)
        self._identity = oci.identity.IdentityClient(config)
        self._namespace = oci.object_storage.ObjectStorageClient(config).get_namespace().data

    def provision(self) -> None:
        subnet_id = self._provision_network()
        repository_name = self._provision_repository()
        self._provision_pull_policy()
        image = self._build_and_push_image(repository_name)
        availability_domain = self._identity.list_availability_domains(compartment_id=self._tenancy_id).data[0].name

        print("\nAdd these keys to the oci_raw [[worker_manager]] section of config.toml:\n")
        print(f'oci_region = "{self._region}"')
        print(f'compartment_id = "{self._compartment_id}"')
        print(f'availability_domain = "{availability_domain}"')
        print(f'subnet_id = "{subnet_id}"')
        print(f'container_image = "{image}"')

    def cleanup(self) -> None:
        vcn = self._find(self._network.list_vcns, "vcn")
        if vcn is not None:
            self._delete_network(vcn.id)

        repository = self._find(self._artifacts.list_container_repositories, "worker")
        if repository is not None:
            self._artifacts_ops.delete_container_repository_and_wait_for_state(
                repository.id, wait_for_states=["DELETED"]
            )
            logger.info(f"Deleted OCIR repository {repository.display_name}")

        policy = self._find_policy()
        if policy is not None:
            self._identity.delete_policy(policy.id)
            logger.info(f"Deleted IAM policy {policy.name}")

    def _provision_network(self) -> str:
        vcn = (
            self._find(self._network.list_vcns, "vcn")
            or self._network_ops.create_vcn_and_wait_for_state(
                oci.core.models.CreateVcnDetails(
                    compartment_id=self._compartment_id, cidr_blocks=[DEFAULT_VCN_CIDR], display_name=self._name("vcn")
                ),
                wait_for_states=[AVAILABLE_STATE],
            ).data
        )
        logger.info(f"VCN {vcn.display_name} is ready")

        gateway = (
            self._find(self._network.list_internet_gateways, "igw", vcn_id=vcn.id)
            or self._network_ops.create_internet_gateway_and_wait_for_state(
                oci.core.models.CreateInternetGatewayDetails(
                    compartment_id=self._compartment_id, vcn_id=vcn.id, is_enabled=True, display_name=self._name("igw")
                ),
                wait_for_states=[AVAILABLE_STATE],
            ).data
        )

        route_table = (
            self._find(self._network.list_route_tables, "routes", vcn_id=vcn.id)
            or self._network_ops.create_route_table_and_wait_for_state(
                oci.core.models.CreateRouteTableDetails(
                    compartment_id=self._compartment_id,
                    vcn_id=vcn.id,
                    display_name=self._name("routes"),
                    route_rules=[
                        oci.core.models.RouteRule(
                            destination=ANYWHERE_CIDR, destination_type="CIDR_BLOCK", network_entity_id=gateway.id
                        )
                    ],
                ),
                wait_for_states=[AVAILABLE_STATE],
            ).data
        )

        # Workers only dial out, so the only inbound traffic allowed is from inside the VCN (a scheduler VM there)
        security_list = (
            self._find(self._network.list_security_lists, "security", vcn_id=vcn.id)
            or self._network_ops.create_security_list_and_wait_for_state(
                oci.core.models.CreateSecurityListDetails(
                    compartment_id=self._compartment_id,
                    vcn_id=vcn.id,
                    display_name=self._name("security"),
                    egress_security_rules=[
                        oci.core.models.EgressSecurityRule(destination=ANYWHERE_CIDR, protocol=ALL_PROTOCOLS)
                    ],
                    ingress_security_rules=[
                        oci.core.models.IngressSecurityRule(source=DEFAULT_VCN_CIDR, protocol=ALL_PROTOCOLS)
                    ],
                ),
                wait_for_states=[AVAILABLE_STATE],
            ).data
        )

        subnet = (
            self._find(self._network.list_subnets, "subnet", vcn_id=vcn.id)
            or self._network_ops.create_subnet_and_wait_for_state(
                oci.core.models.CreateSubnetDetails(
                    compartment_id=self._compartment_id,
                    vcn_id=vcn.id,
                    cidr_block=DEFAULT_SUBNET_CIDR,
                    display_name=self._name("subnet"),
                    route_table_id=route_table.id,
                    security_list_ids=[security_list.id],
                ),
                wait_for_states=[AVAILABLE_STATE],
            ).data
        )
        logger.info(f"Subnet {subnet.display_name} is ready")
        return subnet.id

    def _delete_network(self, vcn_id: str) -> None:
        # A subnet goes before the route table and security list it uses, and all of them before the VCN
        for list_resources, suffix, delete_and_wait in (
            (self._network.list_subnets, "subnet", self._network_ops.delete_subnet_and_wait_for_state),
            (self._network.list_security_lists, "security", self._network_ops.delete_security_list_and_wait_for_state),
            (self._network.list_route_tables, "routes", self._network_ops.delete_route_table_and_wait_for_state),
            (self._network.list_internet_gateways, "igw", self._network_ops.delete_internet_gateway_and_wait_for_state),
        ):
            resource = self._find(list_resources, suffix, vcn_id=vcn_id)
            if resource is not None:
                delete_and_wait(resource.id, wait_for_states=["TERMINATED"])
                logger.info(f"Deleted {resource.display_name}")

        self._network_ops.delete_vcn_and_wait_for_state(vcn_id, wait_for_states=["TERMINATED"])
        logger.info(f"Deleted {self._name('vcn')}")

    def _provision_repository(self) -> str:
        repository = (
            self._find(self._artifacts.list_container_repositories, "worker")
            or self._artifacts_ops.create_container_repository_and_wait_for_state(
                oci.artifacts.models.CreateContainerRepositoryDetails(
                    compartment_id=self._compartment_id, display_name=self._name("worker"), is_public=False
                ),
                wait_for_states=[AVAILABLE_STATE],
            ).data
        )
        logger.info(f"OCIR repository {repository.display_name} is ready")
        return repository.display_name

    def _provision_pull_policy(self) -> None:
        if self._find_policy() is not None:
            return

        statement = (
            f"Allow any-user to read repos in compartment id {self._compartment_id} where ALL "
            f"{{request.principal.type = 'computecontainerinstance', "
            f"request.principal.compartment.id = '{self._compartment_id}'}}"
        )
        self._identity.create_policy(
            oci.identity.models.CreatePolicyDetails(
                compartment_id=self._compartment_id,
                name=self._name("pull"),
                statements=[statement],
                description="Lets Scaler OCI Raw container instances pull the worker image",
            )
        )
        logger.info(f"Created IAM policy {self._name('pull')}")

    def _build_and_push_image(self, repository_name: str) -> str:
        image = f"{self._region}.ocir.io/{self._namespace}/{repository_name}:latest"
        subprocess.run(
            [
                "docker",
                "build",
                "--platform",
                IMAGE_PLATFORM,
                "-f",
                str(DOCKERFILE_PATH),
                "-t",
                image,
                str(DOCKER_BUILD_CONTEXT),
            ],
            check=True,
        )
        subprocess.run(["docker", "push", image], check=True)
        logger.info(f"Pushed {image}")
        return image

    def _find(self, list_resources: Any, suffix: str, **filters: str) -> Optional[Any]:
        resources = list_resources(
            compartment_id=self._compartment_id,
            display_name=self._name(suffix),
            lifecycle_state=AVAILABLE_STATE,
            **filters,
        ).data
        # OCIR list calls return a collection object, the networking list calls a plain list
        return next(iter(getattr(resources, "items", resources)), None)

    def _find_policy(self) -> Optional[Any]:
        policies = self._identity.list_policies(
            compartment_id=self._compartment_id, name=self._name("pull"), lifecycle_state="ACTIVE"
        ).data
        return next(iter(policies), None)

    def _name(self, suffix: str) -> str:
        return f"{self._prefix}-{suffix}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision OCI resources for the Scaler OCI Raw worker manager")
    parser.add_argument("action", choices=["provision", "cleanup"])
    parser.add_argument("--compartment-id", required=True, help="OCI compartment OCID")
    parser.add_argument("--region", default=DEFAULT_REGION, help=f"OCI region (default: {DEFAULT_REGION})")
    parser.add_argument("--prefix", default=DEFAULT_PREFIX, help=f"Resource name prefix (default: {DEFAULT_PREFIX})")
    parser.add_argument("--profile", default="DEFAULT", help="OCI config file profile (default: DEFAULT)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    provisioner = OCIRawProvisioner(args.compartment_id, args.region, args.prefix, args.profile)
    if args.action == "provision":
        provisioner.provision()
    else:
        provisioner.cleanup()


if __name__ == "__main__":
    main()
