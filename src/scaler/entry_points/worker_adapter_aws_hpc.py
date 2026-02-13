"""
Entry point for AWS HPC Worker Adapter.

Supports multiple AWS HPC backends:
- batch: AWS Batch (EC2 compute environment)
- (future) parallelcluster: AWS ParallelCluster
- (future) lambda: AWS Lambda
"""

import argparse
import logging

from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.config.types.zmq import ZMQConfig
from scaler.utility.logging.utility import setup_logger


def parse_args():
    parser = argparse.ArgumentParser(description="AWS HPC Worker Adapter for Scaler")
    
    # Backend selection
    parser.add_argument(
        "--backend", "-b",
        type=str,
        default="batch",
        choices=["batch"],  # Add more as implemented: "parallelcluster", "lambda"
        help="AWS HPC backend (default: batch)"
    )
    
    # Scheduler connection
    parser.add_argument(
        "--scheduler-address", "-a",
        type=str,
        required=True,
        help="Scheduler address (e.g., tcp://localhost:2345)"
    )
    parser.add_argument(
        "--object-storage-address",
        type=str,
        default=None,
        help="Object storage address (e.g., localhost:2346)"
    )
    
    # AWS Batch configuration
    parser.add_argument(
        "--job-queue", "-q",
        type=str,
        help="AWS Batch job queue name (required for batch backend)"
    )
    parser.add_argument(
        "--job-definition", "-d",
        type=str,
        help="AWS Batch job definition name (required for batch backend)"
    )
    parser.add_argument(
        "--aws-region",
        type=str,
        default="us-east-1",
        help="AWS region"
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        help="S3 bucket for task data (required for batch backend)"
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="scaler-tasks",
        help="S3 prefix for task data"
    )
    
    # Worker configuration
    parser.add_argument(
        "--max-concurrent-jobs",
        type=int,
        default=100,
        help="Maximum concurrent jobs"
    )
    parser.add_argument(
        "--job-timeout",
        type=int,
        default=60,
        help="Job timeout in minutes (default: 60 = 1 hour)"
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=int,
        default=1,
        help="Heartbeat interval in seconds"
    )
    parser.add_argument(
        "--death-timeout",
        type=int,
        default=30,
        help="Death timeout in seconds"
    )
    parser.add_argument(
        "--name",
        type=str,
        default=None,
        help="Worker name (default: aws-<backend>-worker)"
    )
    
    # Logging
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level"
    )
    
    return parser.parse_args()


def create_batch_worker(args, scheduler_address, object_storage_address):
    """Create AWS Batch worker with TaskManager."""
    from scaler.worker_adapter.aws_hpc.worker import AWSBatchWorker
    
    # Validate required args for batch backend
    if not args.job_queue:
        raise ValueError("--job-queue required for batch backend")
    if not args.job_definition:
        raise ValueError("--job-definition required for batch backend")
    if not args.s3_bucket:
        raise ValueError("--s3-bucket required for batch backend")
    
    return AWSBatchWorker(
        name=args.name or "aws-batch-worker",
        address=scheduler_address,
        object_storage_address=object_storage_address,
        job_queue=args.job_queue,
        job_definition=args.job_definition,
        aws_region=args.aws_region,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        base_concurrency=args.max_concurrent_jobs,
        heartbeat_interval_seconds=args.heartbeat_interval,
        death_timeout_seconds=args.death_timeout,
        job_timeout_seconds=args.job_timeout * 60,  # convert minutes to seconds
    )


# Registry of backend factories
BACKEND_FACTORIES = {
    "batch": create_batch_worker,
    # Future backends:
    # "parallelcluster": create_parallelcluster_worker,
    # "lambda": create_lambda_worker,
}


def main():
    args = parse_args()
    
    setup_logger()
    logging.getLogger().setLevel(getattr(logging, args.log_level))
    
    scheduler_address = ZMQConfig.from_string(args.scheduler_address)
    
    object_storage_address = None
    if args.object_storage_address:
        host, port = args.object_storage_address.split(":")
        object_storage_address = ObjectStorageAddressConfig(host, int(port))
    
    # Create worker for selected backend
    factory = BACKEND_FACTORIES.get(args.backend)
    if not factory:
        raise ValueError(f"Unknown backend: {args.backend}")
    
    worker = factory(args, scheduler_address, object_storage_address)
    
    logging.info(f"Starting AWS HPC Worker (backend: {args.backend})")
    logging.info(f"  Scheduler: {args.scheduler_address}")
    if args.backend == "batch":
        logging.info(f"  Job Queue: {args.job_queue}")
        logging.info(f"  Job Definition: {args.job_definition}")
        logging.info(f"  S3: s3://{args.s3_bucket}/{args.s3_prefix}")
    logging.info(f"  Max Concurrent Jobs: {args.max_concurrent_jobs}")
    logging.info(f"  Job Timeout: {args.job_timeout} minutes")
    
    worker.start()
    worker.join()


if __name__ == "__main__":
    main()
