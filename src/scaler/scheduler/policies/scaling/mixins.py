import abc
from typing import List

from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.types import ClusterState
from scaler.utility.identifiers import WorkerManagerID


class ScalingPolicy(metaclass=abc.ABCMeta):
    """Scaling policy interface."""

    @abc.abstractmethod
    def get_scaling_commands(
        self, worker_manager_id: WorkerManagerID, cluster_state: ClusterState
    ) -> List[WorkerManagerCommand]:
        """Pure function: state in, declarative scaling command out."""
        raise NotImplementedError()
