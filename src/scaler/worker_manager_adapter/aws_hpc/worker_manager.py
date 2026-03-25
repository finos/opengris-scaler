import logging

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig, AWSHPCBackend
from scaler.worker_manager_adapter.aws_hpc.worker import AWSBatchWorker


class AWSHPCWorkerManager:
    def __init__(self, config: AWSBatchWorkerManagerConfig) -> None:
        self._config = config

    def run(self) -> None:
        config = self._config
        logging.info(f"Starting AWS HPC Worker Manager (backend: {config.backend.name})")
        if config.backend != AWSHPCBackend.batch:
            raise NotImplementedError(f"backend {config.backend.name!r} is not yet implemented")

        worker = AWSBatchWorker(
            name=config.name or "aws-batch-worker",
            address=config.worker_manager_config.scheduler_address,
            object_storage_address=config.worker_manager_config.object_storage_address,
            job_queue=config.job_queue,
            job_definition=config.job_definition,
            aws_region=config.aws_region,
            s3_bucket=config.s3_bucket,
            s3_prefix=config.s3_prefix,
            base_concurrency=config.max_concurrent_jobs,
            heartbeat_interval_seconds=config.worker_config.heartbeat_interval_seconds,
            death_timeout_seconds=config.worker_config.death_timeout_seconds,
            task_queue_size=config.worker_config.per_worker_task_queue_size,
            io_threads=config.worker_config.io_threads,
            event_loop=config.worker_config.event_loop,
            job_timeout_seconds=config.job_timeout_minutes * 60,
            worker_manager_id=config.worker_manager_id.encode(),
        )
        worker.start()
        worker.join()
