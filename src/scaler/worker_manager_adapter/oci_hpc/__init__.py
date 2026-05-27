"""
OCI HPC Worker Adapter for OpenGRIS Scaler.

Supports OCI HPC backends:
- OCI Container Instances: Receives tasks from scheduler and submits as on-demand container instances

Architecture (TaskManager pattern):
    Scheduler Stream → OCIContainerInstanceWorker → OCIHPCTaskManager → OCI Container Instances
                                        ↓
                                Heartbeats to Scheduler
                                        ↓
                            Poll Results → TaskResult to Scheduler

Components:
    - OCIContainerInstanceWorker: Process connecting to scheduler stream
    - OCIHPCTaskManager: Handles task queuing, priority, and OCI Container Instance submission
    - OCIContainerInstanceHeartbeatManager: Sends heartbeats to scheduler
    - ContainerInstanceJobCallback: Tracks task→instance mappings
    - container_instance_job_runner: Script running inside OCI Container Instances

Service Mapping (AWS → OCI):
    - AWS Batch          → OCI Container Instances
    - Amazon S3          → OCI Object Storage
    - Amazon ECR         → OCI Container Registry (OCIR)
    - Amazon CloudWatch  → OCI Logging
    - AWS IAM Role       → OCI Dynamic Group + IAM Policies
"""

from scaler.worker_manager_adapter.oci_hpc.callback import ContainerInstanceJobCallback
from scaler.worker_manager_adapter.oci_hpc.heartbeat_manager import OCIContainerInstanceHeartbeatManager
from scaler.worker_manager_adapter.oci_hpc.task_manager import OCIHPCTaskManager
from scaler.worker_manager_adapter.oci_hpc.worker import OCIContainerInstanceWorker

__all__ = [
    "OCIContainerInstanceWorker",
    "OCIHPCTaskManager",
    "OCIContainerInstanceHeartbeatManager",
    "ContainerInstanceJobCallback",
]
