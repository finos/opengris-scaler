"""
AWS HPC Worker Adapter for OpenGRIS Scaler.

Supports multiple AWS HPC backends:
- AWS Batch: Receives tasks from scheduler and submits as Batch jobs

Architecture:
    Scheduler Stream → AWSBatchWorker → AWSBatchWorkerAdapter → AWS Batch Jobs
                                ↓
                        Heartbeats to Scheduler
                                ↓
                    Poll Results → TaskResult to Scheduler

Components:
    - AWSBatchWorker: Process connecting to scheduler stream
    - AWSBatchWorkerAdapter: Submits tasks to AWS Batch, polls results
    - AWSBatchHeartbeatManager: Sends heartbeats to scheduler
    - BatchJobCallback: Tracks task→job mappings
    - batch_job_runner: Script running inside AWS Batch containers
"""

from scaler.worker_adapter.aws_hpc.worker_adapter import AWSBatchWorkerAdapter
from scaler.worker_adapter.aws_hpc.worker import AWSBatchWorker
from scaler.worker_adapter.aws_hpc.heartbeat_manager import AWSBatchHeartbeatManager
from scaler.worker_adapter.aws_hpc.callback import BatchJobCallback

__all__ = [
    "AWSBatchWorkerAdapter",
    "AWSBatchWorker",
    "AWSBatchHeartbeatManager",
    "BatchJobCallback",
]
