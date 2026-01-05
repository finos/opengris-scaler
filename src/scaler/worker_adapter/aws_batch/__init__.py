"""
AWS Batch Worker Adapter for OpenGRIS Scaler.

Receives tasks from the Scaler scheduler streaming mechanism and submits
them directly as AWS Batch jobs. Large payloads are compressed to stay
within AWS Batch limits.

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

from scaler.worker_adapter.aws_batch.worker_adapter import AWSBatchWorkerAdapter
from scaler.worker_adapter.aws_batch.worker import AWSBatchWorker
from scaler.worker_adapter.aws_batch.heartbeat_manager import AWSBatchHeartbeatManager
from scaler.worker_adapter.aws_batch.callback import BatchJobCallback

__all__ = [
    "AWSBatchWorkerAdapter",
    "AWSBatchWorker",
    "AWSBatchHeartbeatManager",
    "BatchJobCallback",
]
