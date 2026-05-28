import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scaler.entry_points.worker_manager_oci_hpc import main


class WorkerManagerOCIHPCEntryPointTest(unittest.TestCase):
    @staticmethod
    def _create_config() -> SimpleNamespace:
        return SimpleNamespace(
            logging_config=SimpleNamespace(paths=[], config_file="", level="INFO"),
            worker_manager_config=SimpleNamespace(
                scheduler_address="tcp://127.0.0.1:2345",
                object_storage_address=None,
                worker_manager_id="test-manager",
                max_task_concurrency=-1,
                effective_worker_scheduler_address="tcp://127.0.0.1:2345",
            ),
            worker_config=SimpleNamespace(
                heartbeat_interval_seconds=1,
                death_timeout_seconds=30,
                per_worker_task_queue_size=100,
                io_threads=2,
                event_loop="builtin",
                per_worker_capabilities=SimpleNamespace(capabilities={}),
            ),
            container_instance_config=SimpleNamespace(
                compartment_id="ocid1.compartment.oc1..example",
                availability_domain="Uocm:PHX-AD-1",
                subnet_id="ocid1.subnet.oc1.phx.example",
                container_image="phx.ocir.io/namespace/scaler:latest",
                oci_region="us-ashburn-1",
                instance_shape="CI.Standard.E4.Flex",
                oci_profile="DEFAULT",
                auth_type="instance_principal",
            ),
            object_storage_namespace="namespace",
            object_storage_bucket="bucket",
            object_storage_prefix="scaler-tasks",
            instance_ocpus=1.0,
            instance_memory_gb=6.0,
            base_concurrency=2,
            job_timeout_seconds=600,
        )

    def test_main_creates_and_runs_worker_manager(self) -> None:
        config = self._create_config()
        mock_manager = MagicMock()

        with patch("scaler.entry_points.worker_manager_oci_hpc.OCIHPCWorkerManagerConfig.parse", return_value=config):
            with patch("scaler.entry_points.worker_manager_oci_hpc.setup_logger"):
                with patch(
                    "scaler.entry_points.worker_manager_oci_hpc.OCIHPCWorkerManager", return_value=mock_manager
                ) as mock_class:
                    main()

        mock_class.assert_called_once_with(config)
        mock_manager.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
