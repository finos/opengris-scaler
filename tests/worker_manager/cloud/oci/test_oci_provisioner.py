import asyncio
import concurrent.futures
import threading
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import oci

from scaler.worker_manager.cloud.oci import worker_manager
from scaler.worker_manager.cloud.oci.worker_manager import OCIInstancesWorkerProvisioner

# Threads in the event loop's default executor: few, as on a 2-vCPU head node.
EXECUTOR_THREADS = 2

# How long the fake SDK's default retry strategy sleeps through a throttled create.
SDK_RETRY_SECONDS = 3.0


class _ThrottledContainerInstancesClient:
    """The first create succeeds and the instance is ACTIVE at once; later creates are throttled.

    Like the OCI SDK, a call without an explicit retry strategy retries a 429 itself, holding its thread meanwhile.
    """

    def __init__(self) -> None:
        self._create_count = 0
        self._lock = threading.Lock()
        self._released = threading.Event()

    def release(self) -> None:
        self._released.set()

    def create_container_instance(self, create_container_instance_details: Any, **kwargs: Any) -> Any:
        with self._lock:
            self._create_count += 1
            create_index = self._create_count
        if create_index == 1:
            return MagicMock(data=MagicMock(id="ocid1.computecontainerinstance.oc1..first"))

        if not isinstance(kwargs.get("retry_strategy"), oci.retry.NoneRetryStrategy):
            self._released.wait(SDK_RETRY_SECONDS)
        raise oci.exceptions.ServiceError(status=429, code="TooManyRequests", headers={}, message="throttled")

    def get_container_instance(self, container_instance_id: str, **kwargs: Any) -> Any:
        return MagicMock(data=MagicMock(lifecycle_state="ACTIVE"))

    def delete_container_instance(self, container_instance_id: str, **kwargs: Any) -> Any:
        return MagicMock()


def _make_provisioner(client: _ThrottledContainerInstancesClient) -> OCIInstancesWorkerProvisioner:
    config = MagicMock()
    config.instance_ocpus = 2.0
    config.python_worker_environment.requirements_txt = ""
    config.worker_config.per_worker_capabilities.capabilities = {}
    config.worker_manager_config.object_storage_address = None
    config.container_instance_config.image_pull_username = None
    with patch.object(OCIInstancesWorkerProvisioner, "_initialize_oci_client"):
        provisioner = OCIInstancesWorkerProvisioner(config, max_instances=-1)
    provisioner._container_instances_client = client
    return provisioner


class TestOCIInstancesWorkerProvisioner(unittest.TestCase):
    def test_startup_timeout_comes_from_the_config(self) -> None:
        """A container instance that installs scaler from source may take longer than the default to start."""
        config = MagicMock()
        config.worker_manager_config.unit_startup_timeout_seconds = 1800
        with patch.object(OCIInstancesWorkerProvisioner, "_initialize_oci_client"):
            provisioner = OCIInstancesWorkerProvisioner(config, max_instances=-1)
        self.assertEqual(provisioner.startup_timeout_seconds(), 1800)

    def test_throttled_creates_do_not_starve_the_active_poll(self) -> None:
        """An instance that turns ACTIVE is seen while throttled creates are in flight, not after their retries."""
        client = _ThrottledContainerInstancesClient()
        provisioner = _make_provisioner(client)

        async def scenario() -> None:
            loop = asyncio.get_running_loop()
            loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(max_workers=EXECUTOR_THREADS))
            first = asyncio.create_task(provisioner.create_unit("first"))
            await asyncio.sleep(0.05)  # the first create returns, and its poll waits out the poll interval
            throttled = [
                asyncio.create_task(provisioner.create_unit(f"throttled-{unit_i}"))
                for unit_i in range(EXECUTOR_THREADS * 2)
            ]
            try:
                handle = await asyncio.wait_for(first, timeout=SDK_RETRY_SECONDS / 2)
            finally:
                client.release()
                await asyncio.gather(*throttled, return_exceptions=True)
            self.assertEqual(handle, "ocid1.computecontainerinstance.oc1..first")

        with (
            patch.object(worker_manager, "_OCI_POLL_INTERVAL_SECONDS", 0.2),
            patch.object(worker_manager, "child_link_arguments", return_value=""),
        ):
            asyncio.run(scenario())
