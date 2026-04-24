import asyncio
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

try:
    import boto3
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError('execute "pip install opengris-scaler[orb]" to use ORB AWS EC2 worker Manager') from exc

from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig
from scaler.protocol.capnp import WorkerManagerCommandResponse
from scaler.utility.event_loop import register_event_loop, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.common import format_capabilities
from scaler.worker_manager_adapter.mixins import WorkerProvisioner
from scaler.worker_manager_adapter.worker_manager_runner import WorkerManagerRunner

Status = WorkerManagerCommandResponse.Status

ORB_AWS_EC2_POLLING_INTERVAL_SECONDS = 5
ORB_AWS_EC2_MAX_POLLING_ATTEMPTS = 60


def get_orb_aws_ec2_worker_name(instance_id: str) -> str:
    if instance_id == "${INSTANCE_ID}":
        return "Worker|ORB|${INSTANCE_ID}|${INSTANCE_ID//i-/}"
    tag = instance_id.replace("i-", "")
    return f"Worker|ORB|{instance_id}|{tag}"


class ORBWorkerProvisioner(WorkerProvisioner):
    def __init__(self, config: ORBAWSEC2WorkerAdapterConfig, max_instances: int, sdk: Any, template_id: str) -> None:
        self._config = config
        self._max_instances = max_instances
        self._sdk = sdk
        self._template_id = template_id
        self._workers: Dict[WorkerID, str] = {}

    async def start_worker(self) -> Tuple[List[bytes], Status]:
        if self._max_instances != -1 and len(self._workers) >= self._max_instances:
            logging.warning(
                f"Worker start rejected: at capacity ({len(self._workers)}/{self._max_instances} instances)"
            )
            return [], Status.tooManyWorkers

        logging.info(f"Submitting ORB machine request for template {self._template_id}...")
        try:
            create_response = await self._sdk.create_request(template_id=self._template_id, count=1)
        except Exception:
            logging.exception("ORB create_request failed")
            return [], Status.unknownAction

        request_id = create_response.get("created_request_id") if isinstance(create_response, dict) else None
        if not request_id:
            logging.error(f"ORB create_request returned no request ID. Response: {create_response}")
            return [], Status.unknownAction

        logging.info(f"ORB request {request_id} submitted, polling for instance ID...")
        timeout_seconds = ORB_AWS_EC2_MAX_POLLING_ATTEMPTS * ORB_AWS_EC2_POLLING_INTERVAL_SECONDS
        elapsed = 0

        while elapsed < timeout_seconds:
            await asyncio.sleep(ORB_AWS_EC2_POLLING_INTERVAL_SECONDS)
            elapsed += ORB_AWS_EC2_POLLING_INTERVAL_SECONDS

            try:
                status_response = await self._sdk.get_request_status(request_ids=[request_id])
            except Exception:
                logging.exception(f"ORB get_request_status failed for request {request_id}")
                return [], Status.unknownAction

            requests = status_response.get("requests", []) if isinstance(status_response, dict) else []
            if not requests:
                continue

            req = requests[0] if isinstance(requests[0], dict) else {}
            status = req.get("status", "")
            machine_ids = req.get("machine_ids", [])
            instance_id = machine_ids[0] if machine_ids else None

            if instance_id:
                worker_name = get_orb_aws_ec2_worker_name(instance_id)
                worker_id = WorkerID(worker_name.encode())
                self._workers[worker_id] = instance_id
                logging.info(
                    f"ORB request {request_id} fulfilled: launched worker '{worker_name}' (instance {instance_id})"
                )
                return [bytes(worker_id)], Status.success

            if status.lower() in {"failed", "error", "cancelled", "canceled"}:
                logging.error(f"ORB request {request_id} reached terminal status '{status}' with no instance ID.")
                return [], Status.unknownAction

        logging.error(f"ORB request {request_id} timed out after {timeout_seconds:.0f}s waiting for instance ID.")
        return [], Status.unknownAction

    async def shutdown_workers(self, worker_ids: List[bytes]) -> Tuple[List[bytes], Status]:
        if not worker_ids:
            return [], Status.workerNotFound

        instance_ids = []
        affected_worker_ids = []
        for wid_bytes in worker_ids:
            worker_id = WorkerID(wid_bytes)
            if worker_id not in self._workers:
                logging.warning(f"Worker with ID {wid_bytes!r} does not exist.")
                return [], Status.workerNotFound
            instance_ids.append(self._workers[worker_id])
            affected_worker_ids.append(wid_bytes)

        logging.info(f"Stopping {len(instance_ids)} worker(s): instances {instance_ids}")
        try:
            await self._sdk.create_return_request(machine_ids=instance_ids)
        except Exception as e:
            logging.error(f"Failed to return instances {instance_ids} to ORB: {e}")
            return [], Status.unknownAction

        for wid_bytes in affected_worker_ids:
            del self._workers[WorkerID(wid_bytes)]

        logging.info(f"Successfully stopped {len(affected_worker_ids)} worker(s): instances {instance_ids}")
        return affected_worker_ids, Status.success

    async def terminate_all_workers(self) -> None:
        if not self._workers:
            return
        instance_ids = list(self._workers.values())
        logging.info(f"Terminating {len(instance_ids)} worker group(s)...")
        try:
            await self._sdk.create_return_request(machine_ids=instance_ids)
            logging.info(f"Successfully requested termination of instances: {instance_ids}")
        except Exception as e:
            logging.warning(f"Failed to terminate instances during cleanup: {e}")
        self._workers.clear()


class ORBAWSEC2WorkerAdapter:
    def __init__(self, config: ORBAWSEC2WorkerAdapterConfig) -> None:
        self._config = config
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._event_loop = config.worker_config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

        self._orb_pool: Optional[ORBWorkerProvisioner] = None
        self._runner: Optional[WorkerManagerRunner] = None

        self._ec2: Optional[Any] = None
        self._created_security_group_id: Optional[str] = None
        self._created_key_name: Optional[str] = None
        self._cleaned_up = False
        self._subnet_id: Optional[str] = None

        if config.image_id is None:
            requirements_content = self._load_requirements_content(config.requirements_txt)
            self._validate_requirements(requirements_content)

    def _build_app_config(self) -> dict:
        region = self._config.aws_region
        return {
            "provider": {
                "selection_policy": "FIRST_AVAILABLE",
                "providers": [
                    {"name": "aws-default", "type": "aws", "enabled": True, "priority": 1, "config": {"region": region}}
                ],
                # ORB skips loading strategy defaults (aws_defaults.json) when config_dict is
                # provided, so provider_defaults must be included explicitly here. Without it,
                # get_effective_handlers() returns {} and RunInstances is not in supported_apis.
                # This may be fixed in a more recent version of the ORB SDK.
                "provider_defaults": {
                    "aws": {
                        "handlers": {
                            "RunInstances": {
                                "handler_class": "RunInstancesHandler",
                                "supports_spot": False,
                                "supports_ondemand": True,
                            }
                        }
                    }
                },
            },
            "storage": {"type": "json"},
        }

    async def _setup(self, sdk: Any) -> None:
        region = self._config.aws_region
        self._ec2 = boto3.client("ec2", region_name=region)
        self._subnet_id = self._config.subnet_id or self._discover_default_subnet()

        workers_per_instance = self._discover_vcpu_count(self._config.instance_type)
        mtc = self._config.worker_manager_config.max_task_concurrency
        max_instances = math.ceil(mtc / workers_per_instance) if mtc != -1 else -1
        logging.info(
            f"ORB instance type {self._config.instance_type!r}: {workers_per_instance} vCPUs/instance, "
            f"max_task_concurrency={mtc} → max_instances={max_instances}"
        )

        template_id = os.urandom(8).hex()

        security_group_ids = self._config.security_group_ids
        if not security_group_ids:
            self._create_security_group(template_id)
            security_group_ids = [self._created_security_group_id]

        key_name = self._config.key_name
        if not key_name:
            self._create_key_pair(template_id)
            key_name = self._created_key_name

        user_data = self._create_user_data()
        image_id = self._config.image_id or self._discover_latest_al2023_ami()

        self._orb_pool = ORBWorkerProvisioner(
            config=self._config, max_instances=max_instances, sdk=sdk, template_id=template_id
        )
        self._runner = WorkerManagerRunner(
            address=self._config.worker_manager_config.scheduler_address,
            name="worker_manager_orb_aws_ec2",
            heartbeat_interval_seconds=self._config.worker_config.heartbeat_interval_seconds,
            capabilities=self._config.worker_config.per_worker_capabilities.capabilities,
            max_provisioner_units=max_instances,
            worker_manager_id=self._config.worker_manager_config.worker_manager_id.encode(),
            worker_provisioner=self._orb_pool,
            io_threads=self._config.worker_config.io_threads,
            workers_per_provisioner_unit=workers_per_instance,
        )

        create_result = await sdk.create_template(
            template_id=template_id,
            name=f"opengris-orb-{template_id}",
            image_id=image_id,
            provider_api="RunInstances",
            instance_type=self._config.instance_type,
            max_instances=max_instances,
            provider_name="aws-default",
            machine_types={self._config.instance_type: 1},
            subnet_ids=[self._subnet_id],
            security_group_ids=security_group_ids,
            key_name=key_name,
            user_data=user_data,
        )
        logging.info(f"create_template result: {create_result}")

        validate_result = await sdk.validate_template(template_id=template_id)
        logging.info(f"validate_template result: {validate_result}")

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    async def _run(self) -> None:
        register_event_loop(self._event_loop)

        try:
            from orb import ORBClient as orb
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                'execute "pip install opengris-scaler[orb]" to use ORB AWS EC2 worker Manager'
            ) from exc

        async with orb(app_config=self._build_app_config()) as sdk:
            # setup_logger is called after the ORB context is entered because ORB reconfigures
            # the root logger during __aenter__, which would otherwise suppress scaler log output.
            setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
            await self._setup(sdk)
            try:
                await self._runner.run_in_loop(self._loop)
            except asyncio.CancelledError:
                pass
            finally:
                await self._orb_pool.terminate_all_workers()

    def _cleanup(self) -> None:
        if self._cleaned_up:
            return
        self._cleaned_up = True

        if self._runner is not None:
            self._runner.cleanup()

        logging.info("Starting cleanup of AWS resources...")

        if self._created_security_group_id is not None:
            try:
                logging.info(f"Deleting AWS security group: {self._created_security_group_id}")
                self._ec2.delete_security_group(GroupId=self._created_security_group_id)
            except Exception as e:
                logging.warning(f"Failed to delete security group {self._created_security_group_id}: {e}")

        if self._created_key_name is not None:
            try:
                logging.info(f"Deleting AWS key pair: {self._created_key_name}")
                self._ec2.delete_key_pair(KeyName=self._created_key_name)
            except Exception as e:
                logging.warning(f"Failed to delete key pair {self._created_key_name}: {e}")

        logging.info("Cleanup completed.")

    def __del__(self) -> None:
        self._cleanup()

    def _create_user_data(self) -> str:
        worker_config = self._config.worker_config
        adapter_config = self._config.worker_manager_config

        script = "#!/bin/bash\n"

        if self._config.image_id is None:
            python_version = self._config.python_version
            requirements_txt = self._config.requirements_txt

            requirements_content = self._load_requirements_content(requirements_txt)

            # User data runs as root so no sudo is needed.
            # set -e ensures any install failure aborts the script rather than launching a broken worker.
            script += f"""set -e
dnf update -y
dnf install -y python{python_version} python{python_version}-pip
python{python_version} -m venv /opt/opengris-scaler
/opt/opengris-scaler/bin/python -m pip install --upgrade pip
cat > /tmp/requirements.txt << 'REQUIREMENTS_EOF'
{requirements_content}
REQUIREMENTS_EOF
/opt/opengris-scaler/bin/pip install -r /tmp/requirements.txt
ln -sf /opt/opengris-scaler/bin/scaler_* /usr/local/bin/
set +e

"""

        # --max-task-concurrency is not passed: scaler_worker_manager defaults to cpu_count - 1 workers,
        # where cpu_count is determined by the machine type the user configured in the ORB template.
        script += f"""INSTANCE_ID=$(ec2-metadata --instance-id --quiet)
nohup scaler_worker_manager baremetal_native {self._worker_scheduler_address!r} \\
    --mode fixed \\
    --worker-type ORB \\
    --worker-manager-id "${{INSTANCE_ID}}" \\
    --per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \\
    --heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \\
    --task-timeout-seconds {worker_config.task_timeout_seconds} \\
    --garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \\
    --death-timeout-seconds {worker_config.death_timeout_seconds} \\
    --trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \\
    --event-loop {self._config.worker_config.event_loop} \\
    --io-threads {self._config.worker_config.io_threads}"""

        if worker_config.hard_processor_suspend:
            script += " \\\n    --hard-processor-suspend"

        if adapter_config.object_storage_address:
            script += f" \\\n    --object-storage-address {adapter_config.object_storage_address!r}"

        capabilities = worker_config.per_worker_capabilities.capabilities
        if capabilities:
            cap_str = format_capabilities(capabilities)
            if cap_str.strip():
                script += f" \\\n    --per-worker-capabilities {cap_str}"

        script += " > /var/log/opengris-scaler.log 2>&1 &\n"

        return script

    def _discover_vcpu_count(self, instance_type: str) -> int:
        response = self._ec2.describe_instance_types(InstanceTypes=[instance_type])
        instance_types = response.get("InstanceTypes", [])
        if not instance_types:
            raise RuntimeError(f"Could not retrieve instance type info for {instance_type!r}.")
        return instance_types[0]["VCpuInfo"]["DefaultVCpus"]

    def _discover_latest_al2023_ami(self) -> str:
        response = self._ec2.describe_images(
            Filters=[
                {"Name": "name", "Values": ["al2023-ami-2023.*-kernel-*-x86_64"]},
                {"Name": "root-device-type", "Values": ["ebs"]},
                {"Name": "virtualization-type", "Values": ["hvm"]},
            ],
            Owners=["amazon"],
        )
        images = response.get("Images", [])
        if not images:
            raise RuntimeError("No AL2023 AMI found in the current region.")
        images.sort(key=lambda img: img["CreationDate"], reverse=True)
        ami_id = images[0]["ImageId"]
        logging.info(f"Auto-discovered latest AL2023 AMI: {ami_id}")
        return ami_id

    def _discover_default_subnet(self) -> str:
        vpcs = self._ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
        if not vpcs["Vpcs"]:
            raise RuntimeError("No default VPC found, and no subnet_id provided.")
        default_vpc_id = vpcs["Vpcs"][0]["VpcId"]

        subnets = self._ec2.describe_subnets(Filters=[{"Name": "vpc-id", "Values": [default_vpc_id]}])
        if not subnets["Subnets"]:
            raise RuntimeError(f"No subnets found in default VPC {default_vpc_id}.")

        subnet_id = subnets["Subnets"][0]["SubnetId"]
        logging.info(f"Auto-discovered subnet_id: {subnet_id}")
        return subnet_id

    def _create_security_group(self, template_id: str) -> None:
        subnet_response = self._ec2.describe_subnets(SubnetIds=[self._subnet_id])
        vpc_id = subnet_response["Subnets"][0]["VpcId"]

        group_name = f"opengris-orb-sg-{template_id}"
        sg_response = self._ec2.create_security_group(
            Description="Temporary security group created for OpenGRIS ORB worker adapter",
            GroupName=group_name,
            VpcId=vpc_id,
        )
        self._created_security_group_id = sg_response["GroupId"]
        logging.info(f"Created security group with ID: {self._created_security_group_id}")

    def _create_key_pair(self, template_id: str) -> None:
        key_name = f"opengris-orb-key-{template_id}"
        self._ec2.create_key_pair(KeyName=key_name)
        self._created_key_name = key_name
        logging.info(f"Created key pair: {key_name}")

    @staticmethod
    def _load_requirements_content(requirements_txt: str) -> str:
        if os.path.isfile(requirements_txt):
            with open(requirements_txt) as f:
                return f.read()
        return requirements_txt

    @staticmethod
    def _validate_requirements(requirements_content: str) -> None:
        found_scaler = False
        for line in requirements_content.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("-"):
                continue
            try:
                req = Requirement(line)
                if canonicalize_name(req.name) == "opengris-scaler":
                    found_scaler = True
            except Exception:
                if "://" not in line:
                    raise ValueError(f"Invalid requirement line that would cause pip to fail: {line!r}")

        if not found_scaler:
            raise ValueError(
                "The requirements file must include the 'opengris-scaler' package. "
                "Workers will fail to start without it."
            )
