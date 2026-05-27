import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from scaler.entry_points.worker_manager_oci_hpc import main


class WorkerManagerOCIHPCEntryPointTest(unittest.TestCase):
    @staticmethod
    def _create_config() -> SimpleNamespace:
        return SimpleNamespace(
            logging_config=SimpleNamespace(paths=[], config_file="", level="INFO"),
            worker_adapter_config=SimpleNamespace(
                scheduler_address="tcp://127.0.0.1:2345", object_storage_address=None
            ),
            compartment_id="ocid1.compartment.oc1..example",
            availability_domain="Uocm:PHX-AD-1",
            subnet_id="ocid1.subnet.oc1.phx.example",
            container_image="phx.ocir.io/namespace/scaler:latest",
            oci_region="us-ashburn-1",
            object_storage_namespace="namespace",
            object_storage_bucket="bucket",
            object_storage_prefix="scaler-tasks",
            instance_shape="CI.Standard.E4.Flex",
            instance_ocpus=1.0,
            instance_memory_gb=6.0,
            base_concurrency=2,
            heartbeat_interval_seconds=1,
            death_timeout_seconds=30,
            task_queue_size=100,
            worker_io_threads=2,
            event_loop="builtin",
            job_timeout_seconds=600,
            oci_config_profile="DEFAULT",
            oci_auth_type="instance_principal",
        )

    def test_main_wires_oci_auth_type_to_worker(self) -> None:
        config = self._create_config()
        mock_worker = MagicMock()
        mock_worker_instance = MagicMock()
        mock_worker.return_value = mock_worker_instance

        with patch("scaler.entry_points.worker_manager_oci_hpc.OCIHPCWorkerAdapterConfig.parse", return_value=config):
            with patch("scaler.entry_points.worker_manager_oci_hpc.setup_logger"):
                with patch("scaler.entry_points.worker_manager_oci_hpc.OCIContainerInstanceWorker", mock_worker):
                    main()

        self.assertEqual(mock_worker.call_count, 1)
        self.assertEqual(mock_worker.call_args.kwargs["oci_auth_type"], "instance_principal")
        mock_worker_instance.start.assert_called_once()
        mock_worker_instance.join.assert_called_once()


if __name__ == "__main__":
    unittest.main()
