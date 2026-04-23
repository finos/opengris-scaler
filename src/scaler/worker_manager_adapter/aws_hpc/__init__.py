"""
AWS HPC Worker Manager for OpenGRIS Scaler.

Supports multiple AWS HPC backends:
- AWS Batch: Receives tasks from scheduler and submits as Batch jobs

Architecture (composition pattern):
    Scheduler Stream → WorkerProcess → AWSBatchExecutionBackend → AWS Batch Jobs
                            ↓
                    Heartbeats to Scheduler
                            ↓
                Poll Results → TaskResult to Scheduler

Components:
    - WorkerProcess: Process connecting to scheduler stream
    - AWSBatchExecutionBackend: Handles task queuing, priority, and AWS Batch submission
    - AWSProcessorStatusProvider: Provides processor status for heartbeats
    - BatchJobCallback: Tracks task→job mappings
    - batch_job_runner: Script running inside AWS Batch containers
"""

from scaler.worker_manager_adapter.aws_hpc.callback import BatchJobCallback
from scaler.worker_manager_adapter.aws_hpc.heartbeat_manager import AWSProcessorStatusProvider
from scaler.worker_manager_adapter.aws_hpc.task_manager import AWSBatchExecutionBackend
from scaler.worker_manager_adapter.aws_hpc.worker import create_aws_batch_worker

__all__ = ["create_aws_batch_worker", "AWSBatchExecutionBackend", "AWSProcessorStatusProvider", "BatchJobCallback"]
