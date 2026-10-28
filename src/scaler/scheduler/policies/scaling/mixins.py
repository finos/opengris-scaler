import abc
from typing import Dict, List

from scaler.protocol.capnp import WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.scheduler.policies.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import WorkerID, WorkerManagerID
from scaler.utility.snapshot import InformationSnapshot


class ScalingPolicy(metaclass=abc.ABCMeta):
    """
    Scaling policy interface.

    WorkerManagerController owns the managed workers and passes them in as parameters.
    Policies return a single declarative setDesiredTaskConcurrency command.
    """

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        worker_manager_snapshots: Dict[WorkerManagerID, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        """Pure function: state in, declarative scaling command out."""
        raise NotImplementedError()
