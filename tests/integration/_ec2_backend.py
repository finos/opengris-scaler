"""Gateway wheel server + process entry for the floci-backed EC2 scaling e2e.

Drives the SHIPPED ``ORBAWSEC2WorkerManager`` unmodified against floci's EC2: boto3 pointed at floci
launches a real Amazon Linux 2023 instance whose shipped UserData installs a ``manylinux`` wheel of the
CURRENT source (served over the docker-bridge gateway) and boots a worker that connects back. Only config
differs from a production deploy -- the boto3 endpoint and a requirements string pointing at the gateway
wheel; the provisioning, UserData generation, ORB SDK path, and scaling are all the shipped code.
"""

from __future__ import annotations

import functools
import glob
import http.server
import os
import threading

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# Where scripts/build_cibuildwheel.sh drops the portable current-source wheel; the e2e serves it to the
# instance over the gateway. Override the directory with SCALER_IT_MANYLINUX_WHEEL_DIR.
WHEEL_DIR = os.environ.get("SCALER_IT_MANYLINUX_WHEEL_DIR", os.path.join(_REPO_ROOT, "dist_manylinux"))

_AWS_REGION = "us-east-1"
# t3.medium reports 2 vCPUs via floci's describe_instance_types, which the manager uses as the scale-up
# divisor (workers-per-instance); the launched worker itself sizes to the container's CPUs.
_INSTANCE_TYPE = "t3.medium"


def install_floci_ec2_compat() -> None:
    """Bridge the one place floci diverges from real AWS on the ORB provisioning path.

    For a missing launch-template NAME, floci returns an empty ``LaunchTemplates`` list where real AWS (and
    moto) raise ``InvalidLaunchTemplateName.NotFoundException``. The ORB SDK relies on that exception to fall
    through to *creating* the template; without it, it indexes ``LaunchTemplates[0]`` and dies with
    "list index out of range". Restore the real-AWS behavior so the shipped provisioner runs unchanged.
    Registering in ``BUILTIN_HANDLERS`` makes every later-created botocore client (including the ORB SDK's
    own) pick it up.
    """
    import botocore.exceptions
    import botocore.handlers

    def _stash_requested_names(params, context, **kwargs):
        names = params.get("LaunchTemplateNames")
        if names:
            context["_requested_lt_names"] = names

    def _raise_not_found_on_empty(parsed, context, **kwargs):
        names = (context or {}).get("_requested_lt_names")
        if names and not parsed.get("LaunchTemplates"):
            raise botocore.exceptions.ClientError(
                {
                    "Error": {
                        "Code": "InvalidLaunchTemplateName.NotFoundException",
                        "Message": f"The specified launch template, with name {names}, does not exist.",
                    }
                },
                "DescribeLaunchTemplates",
            )

    botocore.handlers.BUILTIN_HANDLERS.append(
        ("before-parameter-build.ec2.DescribeLaunchTemplates", _stash_requested_names)
    )
    botocore.handlers.BUILTIN_HANDLERS.append(("after-call.ec2.DescribeLaunchTemplates", _raise_not_found_on_empty))


def manylinux_wheel() -> str:
    """Path to the newest current-source manylinux wheel under :data:`WHEEL_DIR`, or ``""`` if none is
    present (the EC2/cross-backend e2es skip themselves then). Built by ``scripts/build_cibuildwheel.sh``."""
    wheels = sorted(glob.glob(os.path.join(WHEEL_DIR, "*manylinux*.whl")))
    return wheels[-1] if wheels else ""


def serve_directory_on_gateway(directory: str, host: str, port: int) -> http.server.ThreadingHTTPServer:
    """Serve ``directory`` over HTTP so a floci EC2 instance can fetch the local wheel over the bridge
    gateway. Runs in a daemon thread; call ``shutdown()`` on the returned server to stop it."""
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=directory)
    server = http.server.ThreadingHTTPServer((host, port), handler)
    threading.Thread(target=server.serve_forever, name="ec2-wheel-server", daemon=True).start()
    return server


def run_ec2_worker_manager(
    scheduler_address: str,
    worker_scheduler_address: str,
    endpoint_url: str,
    wheel_url: str,
    python_version: str,
    max_task_concurrency: int,
    worker_manager_id: str = "wm-ec2-it",
) -> None:
    """Run the real ORB/EC2 worker manager: it connects to the scheduler over loopback and, via boto3 aimed
    at ``endpoint_url`` (floci), launches AL2023 instances whose shipped UserData ``uv pip install``s the
    current-source wheel from ``wheel_url`` (the gateway) and starts a worker that dials back over
    ``worker_scheduler_address``. ``max_task_concurrency`` caps the pool at ``ceil(mtc / vCPUs)`` instances."""
    import tempfile

    # Set on the child only so the shipped manager's boto3 hits floci.
    os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", _AWS_REGION)
    install_floci_ec2_compat()  # before any boto3 client (incl. the ORB SDK's) is created
    # ORB persists templates/config under ORB_ROOT_DIR plus a cwd-relative metrics/ dir; keep both in a
    # temp dir so nothing lands in the repo (the manager process is short-lived, so the dir is disposable).
    orb_root = tempfile.mkdtemp(prefix="scaler-it-orb-")
    os.environ["ORB_ROOT_DIR"] = orb_root
    os.chdir(orb_root)

    from scaler.config.common.python_worker_environment import PythonWorkerEnvironmentConfig
    from scaler.config.common.worker import WorkerConfig
    from scaler.config.common.worker_manager import WorkerManagerConfig
    from scaler.config.section.orb_aws_ec2_worker_manager import ORBAWSEC2WorkerManagerConfig
    from scaler.config.types.address import AddressConfig
    from scaler.config.types.network_backend import NetworkBackendType
    from scaler.config.types.worker import WorkerCapabilities
    from scaler.utility.logging.utility import setup_logger
    from scaler.worker_manager_adapter.orb_aws_ec2.worker_manager import ORBAWSEC2WorkerManager

    setup_logger()
    config = ORBAWSEC2WorkerManagerConfig(
        worker_manager_config=WorkerManagerConfig(
            scheduler_address=AddressConfig.from_string(scheduler_address),
            worker_scheduler_address=AddressConfig.from_string(worker_scheduler_address),
            worker_manager_id=worker_manager_id,
            max_task_concurrency=max_task_concurrency,
        ),
        aws_region=_AWS_REGION,
        image_id=None,  # None triggers the UserData bootstrap; floci launches AL2023 regardless of the id
        python_worker_environment=PythonWorkerEnvironmentConfig(
            python_version=python_version,
            # A plain (non-git) URL requirement takes the shipped UserData's wheel-install path -- no
            # in-instance compile; uv fetches this wheel from the gateway and its deps from PyPI.
            requirements_txt=f"opengris-scaler @ {wheel_url}",
        ),
        instance_type=_INSTANCE_TYPE,
        worker_config=WorkerConfig(per_worker_capabilities=WorkerCapabilities({})),
        # Match the host scheduler (which defaults to ymq); the UserData forwards this to the worker.
        network_backend=NetworkBackendType.ymq,
    )
    ORBAWSEC2WorkerManager(config).run()
