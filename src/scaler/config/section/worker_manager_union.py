from typing import Union

from scaler.config.section.aws_hpc_worker_manager import AWSBatchWorkerManagerConfig
from scaler.config.section.ecs_worker_manager import ECSWorkerManagerConfig
from scaler.config.section.native_worker_manager import NativeWorkerManagerConfig
from scaler.config.section.orb_aws_ec2_worker_adapter import ORBAWSEC2WorkerAdapterConfig
from scaler.config.section.symphony_worker_manager import SymphonyWorkerManagerConfig

WorkerManagerUnion = Union[
    NativeWorkerManagerConfig,
    SymphonyWorkerManagerConfig,
    ECSWorkerManagerConfig,
    AWSBatchWorkerManagerConfig,
    ORBAWSEC2WorkerAdapterConfig,
]
