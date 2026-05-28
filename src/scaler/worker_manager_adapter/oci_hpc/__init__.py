"""
OCI HPC Worker Manager for OpenGRIS Scaler.

Submits each Scaler task as an on-demand OCI Container Instance and reports
results back to the scheduler via the WorkerProcess pattern.

Architecture:
    Scheduler → WorkerManagerRunner → OCIHPCWorkerProvisioner → WorkerProcess
                                                                      ↓
                                                          OCIHPCExecutionBackend
                                                                      ↓
                                                          OCI Container Instances

Service Mapping (AWS → OCI):
    - AWS Batch          → OCI Container Instances
    - Amazon S3          → OCI Object Storage
    - Amazon ECR         → OCI Container Registry (OCIR)
    - Amazon CloudWatch  → OCI Logging
    - AWS IAM Role       → OCI Dynamic Group + IAM Policies
"""

from scaler.worker_manager_adapter.oci_hpc.execution_backend import OCIHPCExecutionBackend
from scaler.worker_manager_adapter.oci_hpc.processor_status import OCIProcessorStatusProvider
from scaler.worker_manager_adapter.oci_hpc.worker import create_oci_hpc_worker
from scaler.worker_manager_adapter.oci_hpc.worker_manager import OCIHPCWorkerManager, OCIHPCWorkerProvisioner

__all__ = [
    "OCIHPCExecutionBackend",
    "OCIHPCWorkerManager",
    "OCIHPCWorkerProvisioner",
    "OCIProcessorStatusProvider",
    "create_oci_hpc_worker",
]
