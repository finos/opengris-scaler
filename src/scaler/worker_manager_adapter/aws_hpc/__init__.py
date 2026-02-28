"""
AWS HPC Worker Adapter for OpenGRIS Scaler.

Supports multiple AWS HPC backends:
- AWS Batch: Receives tasks from scheduler and submits as Batch jobs

Architecture (TaskManager pattern):
    Scheduler Stream → AWSBatchWorker → AWSHPCTaskManager → AWS Batch Jobs
                                ↓
                        Heartbeats to Scheduler
                                ↓
                    Poll Results → TaskResult to Scheduler

Components:
    - AWSBatchWorker: Process connecting to scheduler stream
    - AWSHPCTaskManager: Handles task queuing, priority, and AWS Batch submission
    - AWSBatchHeartbeatManager: Sends heartbeats to scheduler
    - BatchJobCallback: Tracks task→job mappings
    - batch_job_runner: Script running inside AWS Batch containers
"""

from scaler.worker_manager_adapter.aws_hpc.callback import BatchJobCallback
from scaler.worker_manager_adapter.aws_hpc.heartbeat_manager import AWSBatchHeartbeatManager
from scaler.worker_manager_adapter.aws_hpc.task_manager import AWSHPCTaskManager
from scaler.worker_manager_adapter.aws_hpc.worker import AWSBatchWorker

__all__ = ["AWSBatchWorker", "AWSHPCTaskManager", "AWSBatchHeartbeatManager", "BatchJobCallback"]
