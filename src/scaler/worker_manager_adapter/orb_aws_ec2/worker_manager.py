import asyncio
import logging
import os
import signal
from typing import Any, Dict, List, Optional, Tuple

try:
    import boto3
    from packaging.requirements import Requirement
    from packaging.utils import canonicalize_name
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError('execute "pip install opengris-scaler[orb]" to use ORB AWS EC2 worker Manager') from exc

from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig
from scaler.io import ymq
from scaler.io.mixins import AsyncConnector, ConnectorRemoteType, NetworkBackend
from scaler.io.network_backends import get_network_backend_from_env
from scaler.io.utility import generate_identity_from_name
from scaler.protocol.capnp import (
    BaseMessage,
    WorkerManagerCommand,
    WorkerManagerCommandResponse,
    WorkerManagerCommandType,
    WorkerManagerHeartbeat,
    WorkerManagerHeartbeatEcho,
)
from scaler.utility.event_loop import create_async_loop_routine, register_event_loop, run_task_forever
from scaler.utility.identifiers import WorkerID
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.common import format_capabilities

Status = WorkerManagerCommandResponse.Status


# Polling configuration for ORB AWS EC2 machine requests
ORB_AWS_EC2_POLLING_INTERVAL_SECONDS = 5
ORB_AWS_EC2_MAX_POLLING_ATTEMPTS = 60


def get_orb_aws_ec2_worker_name(instance_id: str) -> str:
    """
    Returns the deterministic worker name for an ORB AWS EC2 instance.
    If instance_id is the bash variable '${INSTANCE_ID}', it returns a bash-compatible string.
    """
    if instance_id == "${INSTANCE_ID}":
        return "Worker|ORB|${INSTANCE_ID}|${INSTANCE_ID//i-/}"
    tag = instance_id.replace("i-", "")
    return f"Worker|ORB|{instance_id}|{tag}"


class ORBAWSEC2WorkerAdapter:
    _config: ORBAWSEC2WorkerAdapterConfig
    _sdk: Optional[Any]
    _workers: Dict[WorkerID, str]
    _template_id: str
    _created_security_group_id: Optional[str]
    _created_key_name: Optional[str]
    _ec2: Optional[Any]

    def __init__(self, config: ORBAWSEC2WorkerAdapterConfig):
        self._config = config
        self._address = config.worker_manager_config.scheduler_address
        self._worker_scheduler_address = config.worker_manager_config.effective_worker_scheduler_address
        self._heartbeat_interval_seconds = config.worker_config.heartbeat_interval_seconds
        self._capabilities = config.worker_config.per_worker_capabilities.capabilities
        self._max_task_concurrency = config.worker_manager_config.max_task_concurrency

        self._event_loop = config.worker_config.event_loop
        self._logging_paths = config.logging_config.paths
        self._logging_level = config.logging_config.level
        self._logging_config_file = config.logging_config.config_file

        self._worker_manager_id = config.worker_manager_config.worker_manager_id.encode()

        self._sdk: Optional[Any] = None
        self._ec2: Optional[Any] = None
        self._backend: Optional[NetworkBackend] = None
        self._connector_external: Optional[AsyncConnector] = None
        self._created_security_group_id: Optional[str] = None
        self._created_key_name: Optional[str] = None
        self._cleaned_up = False
        self._workers: Dict[WorkerID, str] = {}
        self._ident: bytes = b"worker_manager_orb_aws_ec2|uninitialized"
        self._subnet_id: Optional[str] = None

        if config.image_id is None:
            requirements_content = self._load_requirements_content(config.requirements_txt)
            self._validate_requirements(requirements_content)

    def _build_app_config(self) -> dict:
        region = self._config.aws_region or "us-east-1"
        return {
            "provider": {
                "selection_policy": "FIRST_AVAILABLE",
                "providers": [
                    {"name": "aws-default", "type": "aws", "enabled": True, "priority": 1, "config": {"region": region}}
                ],
                # ORB skips loading strategy defaults (aws_defaults.json) when config_dict is
                # provided, so provider_defaults must be included explicitly here. Without it,
                # get_effective_handlers() returns {} and RunInstances is not in supported_apis.
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

    async def __setup(self) -> None:
        """Set up AWS resources and the ORB template after the SDK is initialised."""
        region = self._config.aws_region or "us-east-1"
        self._ec2 = boto3.client("ec2", region_name=region)
        self._subnet_id = self._config.subnet_id or self._discover_default_subnet()
        self._template_id = os.urandom(8).hex()

        security_group_ids = self._config.security_group_ids
        if not security_group_ids:
            self._create_security_group()
            security_group_ids = [self._created_security_group_id]

        key_name = self._config.key_name
        if not key_name:
            self._create_key_pair()
            key_name = self._created_key_name

        user_data = self._create_user_data()

        image_id = self._config.image_id or self._discover_latest_al2023_ami()

        create_result = await self._sdk.create_template(
            template_id=self._template_id,
            name=f"opengris-orb-{self._template_id}",
            image_id=image_id,
            provider_api="RunInstances",
            instance_type=self._config.instance_type,
            max_instances=self._config.worker_manager_config.max_task_concurrency,
            provider_name="aws-default",
            machine_types={self._config.instance_type: 1},
            subnet_ids=[self._subnet_id],
            security_group_ids=security_group_ids,
            key_name=key_name,
            user_data=user_data,
        )
        logging.info(f"create_template result: {create_result}")

        validate_result = await self._sdk.validate_template(template_id=self._template_id)
        logging.info(f"validate_template result: {validate_result}")

        self._backend = get_network_backend_from_env()
        self._name = "worker_manager_orb_aws_ec2"
        self._ident = generate_identity_from_name(self._name)

        self._connector_external = self._backend.create_async_connector(
            identity=self._ident, callback=self.__on_receive_external
        )

    async def __terminate_all_workers(self) -> None:
        """Return all active instances to ORB before the SDK context exits."""
        if not self._workers or self._sdk is None:
            return
        instance_ids = list(self._workers.values())
        logging.info(f"Terminating {len(instance_ids)} worker group(s)...")
        try:
            await self._sdk.create_return_request(machine_ids=instance_ids)
            logging.info(f"Successfully requested termination of instances: {instance_ids}")
        except Exception as e:
            logging.warning(f"Failed to terminate instances during cleanup: {e}")
        self._workers.clear()

    async def __on_receive_external(self, message: BaseMessage):
        try:
            if isinstance(message, WorkerManagerCommand):
                await self._handle_command(message)
            elif isinstance(message, WorkerManagerHeartbeatEcho):
                pass
            else:
                logging.warning(f"Received unknown message type: {type(message)}")
        except Exception:
            logging.exception(f"Unhandled exception while processing message {type(message).__name__}")

    async def _handle_command(self, command: WorkerManagerCommand):
        cmd_type = command.command
        response_status: Status = Status.success
        worker_ids: List[bytes] = []
        capabilities: Dict[str, int] = {}

        if cmd_type == WorkerManagerCommandType.startWorkers:
            worker_ids, response_status = await self.start_worker()
            if response_status == Status.success:
                capabilities = self._capabilities
        elif cmd_type == WorkerManagerCommandType.shutdownWorkers:
            worker_ids, response_status = await self.shutdown_workers(list(command.workerIDs))
        else:
            logging.error(f"Received unknown command type: {cmd_type!r}")
            raise ValueError(f"Unknown Command: {cmd_type!r}")

        assert self._connector_external is not None
        await self._connector_external.send(
            WorkerManagerCommandResponse(
                command=cmd_type, status=response_status, workerIDs=worker_ids, capabilities=capabilities
            )
        )

    async def __send_heartbeat(self):
        assert self._connector_external is not None
        await self._connector_external.send(
            WorkerManagerHeartbeat(
                maxTaskConcurrency=self._max_task_concurrency,
                capabilities=self._capabilities,
                workerManagerID=self._worker_manager_id,
            )
        )

    def run(self) -> None:
        self._loop = asyncio.new_event_loop()
        run_task_forever(self._loop, self._run(), cleanup_callback=self._cleanup)

    def __destroy(self):
        logging.info(f"Worker adapter {self._ident!r} received signal, shutting down")
        self._task.cancel()

    def __register_signal(self):
        self._loop.add_signal_handler(signal.SIGINT, self.__destroy)
        self._loop.add_signal_handler(signal.SIGTERM, self.__destroy)

    async def _run(self) -> None:
        register_event_loop(self._event_loop)

        try:
            from orb import ORBClient as orb
        except ModuleNotFoundError as exc:
            raise ModuleNotFoundError(
                'execute "pip install opengris-scaler[orb]" to use ORB AWS EC2 worker Manager'
            ) from exc

        async with orb(app_config=self._build_app_config()) as sdk:
            self._sdk = sdk
            # setup_logger is called after the ORB context is entered because ORB reconfigures
            # the root logger during __aenter__, which would otherwise suppress scaler log output.
            setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
            await self.__setup()
            self._task = self._loop.create_task(self.__get_loops())
            self.__register_signal()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            finally:
                await self.__terminate_all_workers()

        self._sdk = None

    async def __get_loops(self):
        assert self._connector_external is not None
        await self._connector_external.connect(self._address, ConnectorRemoteType.Binder)

        loops = [
            create_async_loop_routine(self._connector_external.routine, 0),
            create_async_loop_routine(self.__send_heartbeat, self._heartbeat_interval_seconds),
        ]

        try:
            await asyncio.gather(*loops)
        except asyncio.CancelledError:
            pass
        except ymq.YMQException as e:
            if e.code == ymq.ErrorCode.ConnectorSocketClosedByRemoteEnd:
                pass
            else:
                logging.exception(f"{self._ident!r}: failed with unhandled exception:\n{e}")
        except Exception:
            logging.exception(f"{self._ident!r}: failed with unhandled exception")

    @staticmethod
    def _load_requirements_content(requirements_txt: str) -> str:
        """Return requirements content from a file path or a literal string."""
        if os.path.isfile(requirements_txt):
            with open(requirements_txt) as f:
                return f.read()
        return requirements_txt

    @staticmethod
    def _validate_requirements(requirements_content: str) -> None:
        """Raise ValueError if the requirements content is invalid or does not include opengris-scaler.

        Each non-comment, non-flag line must be parseable as a PEP 508 requirement or a direct URL
        reference (containing '://'). Lines that fail both checks would cause `pip install` to fail
        inside the EC2 userdata script.
        """
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

    def _create_user_data(self) -> str:
        worker_config = self._config.worker_config
        adapter_config = self._config.worker_manager_config

        script = "#!/bin/bash\n"

        if self._config.image_id is None:
            python_version = self._config.python_version
            requirements_txt = self._config.requirements_txt

            requirements_content = self._load_requirements_content(requirements_txt)

            # Phase 1: install Python and dependencies. User data runs as root so no sudo is needed.
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

        # Phase 2: launch the worker manager.
        # NOTE: --max-task-concurrency is not passed; scaler_worker_manager defaults to cpu_count - 1 workers,
        # where cpu_count is determined by the machine type configured by the user.
        script += f"""INSTANCE_ID=$(ec2-metadata --instance-id --quiet)
nohup scaler_worker_manager baremetal_native {self._worker_scheduler_address!r} \
    --mode fixed \
    --worker-type ORB \
    --worker-manager-id "${{INSTANCE_ID}}" \
    --per-worker-task-queue-size {worker_config.per_worker_task_queue_size} \
    --heartbeat-interval-seconds {worker_config.heartbeat_interval_seconds} \
    --task-timeout-seconds {worker_config.task_timeout_seconds} \
    --garbage-collect-interval-seconds {worker_config.garbage_collect_interval_seconds} \
    --death-timeout-seconds {worker_config.death_timeout_seconds} \
    --trim-memory-threshold-bytes {worker_config.trim_memory_threshold_bytes} \
    --event-loop {self._config.worker_config.event_loop} \
    --io-threads {self._config.worker_config.io_threads}"""

        if worker_config.hard_processor_suspend:
            script += " \
    --hard-processor-suspend"

        if adapter_config.object_storage_address:
            script += f" \
    --object-storage-address {adapter_config.object_storage_address!r}"

        capabilities = worker_config.per_worker_capabilities.capabilities
        if capabilities:
            cap_str = format_capabilities(capabilities)
            if cap_str.strip():
                script += f" \
    --per-worker-capabilities {cap_str}"

        script += " > /var/log/opengris-scaler.log 2>&1 &\n"

        return script

    def _discover_latest_al2023_ami(self) -> str:
        """Discover the most recent Amazon Linux 2023 x86_64 EBS HVM AMI in the configured region."""
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

    def _create_security_group(self):
        # Get VPC ID from Subnet
        subnet_response = self._ec2.describe_subnets(SubnetIds=[self._subnet_id])
        vpc_id = subnet_response["Subnets"][0]["VpcId"]

        # Create Security Group (outbound-only — workers connect out to scheduler via ZMQ)
        group_name = f"opengris-orb-sg-{self._template_id}"
        sg_response = self._ec2.create_security_group(
            Description="Temporary security group created for OpenGRIS ORB worker adapter",
            GroupName=group_name,
            VpcId=vpc_id,
        )
        self._created_security_group_id = sg_response["GroupId"]
        logging.info(f"Created security group with ID: {self._created_security_group_id}")

    def _create_key_pair(self):
        key_name = f"opengris-orb-key-{self._template_id}"
        self._ec2.create_key_pair(KeyName=key_name)
        self._created_key_name = key_name
        logging.info(f"Created key pair: {key_name}")

    def _cleanup(self):
        if self._cleaned_up:
            return
        self._cleaned_up = True

        if self._connector_external is not None:
            self._connector_external.destroy()

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

    def __del__(self):
        self._cleanup()

    async def start_worker(self) -> Tuple[List[bytes], Status]:
        if len(self._workers) >= self._max_task_concurrency != -1:
            logging.warning(
                f"Worker start rejected: at capacity ({len(self._workers)}/{self._max_task_concurrency} workers)"
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
