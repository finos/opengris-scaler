"""Tool-agnostic mocked-AWS control plane for the integration skeleton.

Two backends are supported behind one interface, selected by the
``SCALER_E2E_AWS_BACKEND`` environment variable:

* ``moto`` (default) -- an in-process boto3 mock. No Docker, no network, runs on
  Linux/macOS/Windows CI. It faithfully backs the AWS *control plane* (the ECS
  ``run_task`` / ``stop_task`` API calls the worker manager makes) but does NOT boot
  the resulting containers, so provisioned "instances" never connect back as workers.
* ``localstack`` -- a real LocalStack container reachable at ``AWS_ENDPOINT_URL``
  (default ``http://localhost:4566``). NOTE: ECS is a LocalStack *Pro* feature, so the ECS
  control-plane test skips itself on the free community image (the EC2 seam still runs);
  LocalStack Pro's ECS/EC2 Docker backend can additionally launch containers for a true
  data-plane run. See README and scripts/run_integration_localstack.sh.

Both backends are seeded through the same plain boto3 calls, so a test written against
this harness is identical regardless of backend.

Why this seam and not just ``MagicMock``: the existing provisioner unit tests already
patch boto3 with ``MagicMock``. This harness instead exercises the real boto3 request
path against a real (mocked) AWS state machine, so it catches wrong parameters, missing
resources, and lifecycle bugs a MagicMock cannot.
"""

from __future__ import annotations

import dataclasses
import os
import uuid
from typing import Any, Dict, List, Optional

import boto3

# moto reads this lazily when a managed policy is attached; it MUST be set before the
# first attach_role_policy call. The ECS provisioner attaches the AWS-managed
# AmazonECSTaskExecutionRolePolicy, which moto only knows about when this is enabled.
os.environ.setdefault("MOTO_IAM_LOAD_MANAGED_POLICIES", "true")

DEFAULT_REGION = "us-east-1"
DEFAULT_CLUSTER = "scaler-it-cluster"
DEFAULT_TASK_DEFINITION = "scaler-it-task-definition"
LOCALSTACK_DEFAULT_ENDPOINT = "http://localhost:4566"


class ECSNotAvailable(RuntimeError):
    """Selected backend has no ECS (community LocalStack: "not yet implemented or pro feature"); moto
    and LocalStack Pro do. Tests catch this and skip instead of failing."""


def is_service_unavailable(exc: Exception) -> bool:
    """True when a backend reports a service as unimplemented or gated behind a paid tier (e.g. a
    Pro-only service on community LocalStack). Matches the several phrasings LocalStack has used across
    versions so the ECS/Batch tests skip (not error) regardless of the exact wording."""
    message = str(exc).lower()
    return any(
        phrase in message for phrase in ("not yet implemented", "not yet been emulated", "pro feature", "license plan")
    )


def selected_backend() -> str:
    return os.environ.get("SCALER_E2E_AWS_BACKEND", "moto").strip().lower()


# Injected into ECS/Batch calls that omit fields moto requires but real AWS defaults. Module-global
# because the botocore handlers below are shared by every client, including the provisioner's own.
_DEFAULT_SECURITY_GROUPS: List[str] = []  # ECS run_task securityGroups (real AWS defaults it)
_BATCH_SERVICE_ROLE_ARN: Optional[str] = None  # Batch CreateComputeEnvironment serviceRole
_COMPAT_INSTALLED = False


def _install_aws_compat() -> None:
    """Bridge the small gaps where moto is stricter than real AWS, so provisioner code runs unchanged.

    Registering handlers in ``BUILTIN_HANDLERS`` makes every later-created botocore client (including
    the ones the provisioner constructs internally) pick them up.
    """
    global _COMPAT_INSTALLED
    if _COMPAT_INSTALLED:
        return
    import botocore.handlers

    def _inject_security_groups(params, **kwargs):
        # ECS worker manager calls run_task with only subnets + assignPublicIp; real AWS defaults the
        # security group, moto raises KeyError.
        awsvpc = (params.get("networkConfiguration") or {}).get("awsvpcConfiguration")
        if awsvpc is not None and not awsvpc.get("securityGroups"):
            awsvpc["securityGroups"] = list(_DEFAULT_SECURITY_GROUPS)

    def _inject_service_role(params, **kwargs):
        # Batch provisioner omits serviceRole (real AWS auto-creates the service-linked role); moto
        # requires it.
        if not params.get("serviceRole") and _BATCH_SERVICE_ROLE_ARN:
            params["serviceRole"] = _BATCH_SERVICE_ROLE_ARN

    botocore.handlers.BUILTIN_HANDLERS.append(("before-parameter-build.ecs.RunTask", _inject_security_groups))
    botocore.handlers.BUILTIN_HANDLERS.append(
        ("before-parameter-build.batch.CreateComputeEnvironment", _inject_service_role)
    )
    _COMPAT_INSTALLED = True


@dataclasses.dataclass
class SeededECSEnvironment:
    region: str
    cluster: str
    task_definition: str
    subnets: List[str]


@dataclasses.dataclass
class SeededEC2Environment:
    region: str
    subnets: List[str]
    security_groups: List[str]
    key_name: str
    image_id: str


class MockedAWS:
    """Context manager yielding a mocked AWS control plane and boto3 client factory."""

    def __init__(self, region: str = DEFAULT_REGION) -> None:
        self.region = region
        self._backend = selected_backend()
        self._moto_ctx: Optional[Any] = None
        self._endpoint_url: Optional[str] = None
        self._saved_env: Dict[str, Optional[str]] = {}

    def __enter__(self) -> "MockedAWS":
        _install_aws_compat()
        if self._backend == "moto":
            from moto import mock_aws

            self._moto_ctx = mock_aws()
            self._moto_ctx.__enter__()
            # moto needs *some* credentials present; they are never validated.
            self._set_env("AWS_ACCESS_KEY_ID", "testing")
            self._set_env("AWS_SECRET_ACCESS_KEY", "testing")
            self._set_env("AWS_SECURITY_TOKEN", "testing")
            self._set_env("AWS_SESSION_TOKEN", "testing")
            self._set_env("AWS_DEFAULT_REGION", self.region)
        else:  # localstack
            self._endpoint_url = os.environ.get("AWS_ENDPOINT_URL", LOCALSTACK_DEFAULT_ENDPOINT)
            self._set_env("AWS_ENDPOINT_URL", self._endpoint_url)
            self._set_env("AWS_DEFAULT_REGION", self.region)
            self._set_env("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "test"))
            self._set_env("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "test"))
        return self

    def __exit__(self, *exc) -> None:
        if self._moto_ctx is not None:
            self._moto_ctx.__exit__(*exc)
            self._moto_ctx = None
        for key, previous in self._saved_env.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
        self._saved_env.clear()

    def _set_env(self, key: str, value: str) -> None:
        self._saved_env.setdefault(key, os.environ.get(key))
        os.environ[key] = value

    def client(self, service: str):
        kwargs = {"region_name": self.region}
        if self._endpoint_url is not None:
            kwargs["endpoint_url"] = self._endpoint_url
        return boto3.client(service, **kwargs)

    def seed_ecs_environment(
        self,
        cluster: str = DEFAULT_CLUSTER,
        task_definition: str = DEFAULT_TASK_DEFINITION,
        container_image: str = "opengris-scaler:it",
        task_cpu: int = 4,
        task_memory_mb: int = 8192,
    ) -> SeededECSEnvironment:
        """Create the VPC/subnet/SG/cluster/task-def a real ECS account would already have.

        Pre-creating the task definition (with container-level memory) makes the provisioner
        *discover* it and skip its own registration -- which both mirrors production and
        avoids a moto Fargate memory-summing quirk.
        """
        ec2 = self.client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        # Fargate awsvpc tasks get an ENI whose private DNS name is only populated when the
        # VPC has DNS hostnames enabled (matches real AWS; avoids a moto ENI AttributeError).
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
        subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName="scaler-it-sg", Description="scaler integration test", VpcId=vpc_id
        )["GroupId"]

        global _DEFAULT_SECURITY_GROUPS
        _DEFAULT_SECURITY_GROUPS = [sg_id]

        import botocore.exceptions

        ecs = self.client("ecs")
        try:
            ecs.create_cluster(clusterName=cluster)
            ecs.register_task_definition(
                family=task_definition,
                cpu=str(task_cpu * 1024),
                memory=str(task_memory_mb),
                networkMode="awsvpc",
                requiresCompatibilities=["FARGATE"],
                containerDefinitions=[
                    {
                        "name": "scaler-container",
                        "image": container_image,
                        "essential": True,
                        "memory": task_memory_mb,
                        "cpu": task_cpu * 1024,
                    }
                ],
            )
        except botocore.exceptions.ClientError as exc:
            if is_service_unavailable(exc):
                raise ECSNotAvailable(str(exc)) from exc
            raise
        return SeededECSEnvironment(
            region=self.region, cluster=cluster, task_definition=task_definition, subnets=[subnet_id]
        )

    def seed_ec2_environment(self, key_name: Optional[str] = None) -> SeededEC2Environment:
        """Create the VPC/subnet/security-group/key-pair/AMI a real EC2 account would have.

        EC2 is available in LocalStack's free community tier (as an in-memory mock VM manager), so
        this seam works on moto AND free LocalStack -- unlike ECS/Batch which are LocalStack Pro.

        Resource names get a unique suffix because LocalStack persists state across tests within a
        container (moto resets per test), so fixed names would collide on the second seed.
        """
        suffix = uuid.uuid4().hex[:8]
        key_name = key_name or f"scaler-it-key-{suffix}"

        ec2 = self.client("ec2")
        vpc_id = ec2.create_vpc(CidrBlock="10.0.0.0/16")["Vpc"]["VpcId"]
        ec2.modify_vpc_attribute(VpcId=vpc_id, EnableDnsHostnames={"Value": True})
        subnet_id = ec2.create_subnet(VpcId=vpc_id, CidrBlock="10.0.1.0/24")["Subnet"]["SubnetId"]
        sg_id = ec2.create_security_group(
            GroupName=f"scaler-it-ec2-sg-{suffix}", Description="scaler integration test", VpcId=vpc_id
        )["GroupId"]
        ec2.create_key_pair(KeyName=key_name)

        images = ec2.describe_images(Owners=["amazon"]).get("Images", [])
        image_id = images[0]["ImageId"] if images else "ami-0abcdef1234567890"

        return SeededEC2Environment(
            region=self.region, subnets=[subnet_id], security_groups=[sg_id], key_name=key_name, image_id=image_id
        )

    def seed_batch_service_role(self, role_name: str = "scaler-it-batch-service-role") -> str:
        """Create the AWS Batch service role and register it for CreateComputeEnvironment injection.

        Real AWS auto-creates the Batch service-linked role; moto requires an explicit serviceRole,
        so the harness pre-creates one and the compat handler injects it. Returns the role ARN.
        """
        import json

        import botocore.exceptions

        global _BATCH_SERVICE_ROLE_ARN
        iam = self.client("iam")
        trust = {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Principal": {"Service": "batch.amazonaws.com"}, "Action": "sts:AssumeRole"}
            ],
        }
        try:
            arn = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))["Role"]["Arn"]
        except botocore.exceptions.ClientError as exc:
            if "EntityAlreadyExists" in str(exc):
                arn = iam.get_role(RoleName=role_name)["Role"]["Arn"]
            else:
                raise
        _BATCH_SERVICE_ROLE_ARN = arn
        return arn
