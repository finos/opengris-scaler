import logging

from scaler.config.section.oci_hpc_worker_manager import OCIHPCWorkerManagerConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.oci_hpc.worker_manager import OCIHPCWorkerManager


def main():
    config = OCIHPCWorkerManagerConfig.parse("Scaler OCI HPC Worker Manager", "oci_hpc_worker_manager")

    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    logging.info("Starting OCI HPC Worker Manager")
    logging.info(f"  Scheduler: {config.worker_manager_config.scheduler_address}")
    logging.info(f"  Compartment: {config.container_instance_config.compartment_id}")
    logging.info(f"  Region: {config.container_instance_config.oci_region}")
    logging.info(f"  Object Storage: oci://{config.object_storage_bucket}/{config.object_storage_prefix}")
    logging.info(f"  Container Image: {config.container_instance_config.container_image}")
    logging.info(f"  Max Concurrent Jobs: {config.base_concurrency}")
    logging.info(f"  Job Timeout: {config.job_timeout_seconds}s")

    OCIHPCWorkerManager(config).run()


if __name__ == "__main__":
    main()
