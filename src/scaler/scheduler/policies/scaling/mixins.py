import abc
from typing import Dict, List

from scaler.protocol.capnp import WorkerManagerCommand
from scaler.scheduler.types import WorkerManagerInfo
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
        worker_manager_id: WorkerManagerID,
        information_snapshot: InformationSnapshot,
        managed_worker_ids: List[WorkerID],
        worker_manager_infos: Dict[WorkerManagerID, WorkerManagerInfo],
        worker_count_by_manager: Dict[WorkerManagerID, int],
    ) -> List[WorkerManagerCommand]:
        """Pure function: state in, declarative scaling command out."""
        raise NotImplementedError()
