import logging

from scaler.config.section.oci_hpc_worker_adapter import OCIHPCWorkerAdapterConfig
from scaler.utility.logging.utility import setup_logger
from scaler.worker_manager_adapter.oci_hpc.worker import OCIContainerInstanceWorker


def main():
    config = OCIHPCWorkerAdapterConfig.parse("Scaler OCI HPC Worker Adapter", "oci_hpc_worker_adapter")

    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    logging.info("Starting OCI HPC Worker Adapter")
    logging.info(f"  Scheduler: {config.worker_adapter_config.scheduler_address}")
    logging.info(f"  Compartment: {config.compartment_id}")
    logging.info(f"  Region: {config.oci_region}")
    logging.info(f"  Object Storage: oci://{config.object_storage_bucket}/{config.object_storage_prefix}")
    logging.info(f"  Container Image: {config.container_image}")
    logging.info(f"  Max Concurrent Jobs: {config.base_concurrency}")
    logging.info(f"  Job Timeout: {config.job_timeout_seconds}s")

    worker = OCIContainerInstanceWorker(
        name="oci-hpc-worker",
        address=config.worker_adapter_config.scheduler_address,
        object_storage_address=config.worker_adapter_config.object_storage_address,
        compartment_id=config.compartment_id,
        availability_domain=config.availability_domain,
        subnet_id=config.subnet_id,
        container_image=config.container_image,
        oci_region=config.oci_region,
        object_storage_namespace=config.object_storage_namespace,
        object_storage_bucket=config.object_storage_bucket,
        object_storage_prefix=config.object_storage_prefix,
        instance_shape=config.instance_shape,
        instance_ocpus=config.instance_ocpus,
        instance_memory_gb=config.instance_memory_gb,
        base_concurrency=config.base_concurrency,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
        death_timeout_seconds=config.death_timeout_seconds,
        task_queue_size=config.task_queue_size,
        io_threads=config.worker_io_threads,
        event_loop=config.event_loop,
        job_timeout_seconds=config.job_timeout_seconds,
        oci_config_profile=config.oci_config_profile,
    )

    worker.start()
    worker.join()


if __name__ == "__main__":
    main()
