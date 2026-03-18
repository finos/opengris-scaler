"""
Entry point for AWS HPC Worker Manager.

Supports multiple AWS HPC backends:
- batch: AWS Batch (EC2 compute environment)
- (future) parallelcluster: AWS ParallelCluster
- (future) lambda: AWS Lambda
"""

import logging
import multiprocessing

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig, AWSHPCBackend
from scaler.utility.logging.utility import setup_logger


def _create_batch_worker(config: AWSBatchWorkerManagerConfig) -> multiprocessing.Process:
    from scaler.worker_manager_adapter.aws_hpc.worker import AWSBatchWorker

    logging.info(f"  Job Queue: {config.job_queue}")
    logging.info(f"  Job Definition: {config.job_definition}")
    logging.info(f"  S3: s3://{config.s3_bucket}/{config.s3_prefix}")
    logging.info(f"  Max Concurrent Jobs: {config.max_concurrent_jobs}")
    logging.info(f"  Job Timeout: {config.job_timeout_minutes} minutes")

    return AWSBatchWorker(
        name=config.name or "aws-batch-worker",
        address=config.worker_manager_config.scheduler_address,
        object_storage_address=config.worker_manager_config.object_storage_address,
        job_queue=config.job_queue,
        job_definition=config.job_definition,
        aws_region=config.aws_region,
        s3_bucket=config.s3_bucket,
        s3_prefix=config.s3_prefix,
        base_concurrency=config.max_concurrent_jobs,
        heartbeat_interval_seconds=config.heartbeat_interval_seconds,
        death_timeout_seconds=config.death_timeout_seconds,
        task_queue_size=config.task_queue_size,
        io_threads=config.worker_io_threads,
        event_loop=config.event_loop,
        job_timeout_seconds=config.job_timeout_minutes * 60,
        worker_manager_id=config.worker_manager_id.encode(),
    )


def main():
    config = AWSBatchWorkerManagerConfig.parse("Scaler AWS HPC Worker Manager", "aws_hpc_worker_manager")

    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    logging.info(f"Starting AWS HPC Worker Manager (backend: {config.backend.name})")
    logging.info(f"  Scheduler: {config.worker_manager_config.scheduler_address}")

    if config.backend == AWSHPCBackend.batch:
        worker = _create_batch_worker(config)
    else:
        raise NotImplementedError(f"backend {config.backend.name!r} is not yet implemented")

    worker.start()
    worker.join()


if __name__ == "__main__":
    main()
