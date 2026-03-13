import abc
from typing import Dict, List

from scaler.protocol.python.message import InformationSnapshot, WorkerManagerCommand, WorkerManagerHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerManagerSnapshot
from scaler.utility.identifiers import WorkerID


class ScalingPolicy:
    """
    Stateless scaling policy interface.

    All state (managed workers, capabilities) is owned by WorkerManagerController and passed in as parameters.
    Policies return commands rather than mutating internal state.
    """

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        worker_manager_heartbeat: WorkerManagerHeartbeat,
        managed_worker_ids: List[WorkerID],
        managed_worker_capabilities: Dict[str, int],
        worker_manager_snapshots: Dict[bytes, WorkerManagerSnapshot],
    ) -> List[WorkerManagerCommand]:
        """
        Pure function: state in, commands out.

        Returns a list of WorkerManagerCommands. Commands are either all start or all shutdown, never mixed.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self, managed_workers: Dict[bytes, List[WorkerID]]) -> ScalingManagerStatus:
        """Pure function: state in, status out."""
        raise NotImplementedError()
