import abc
from typing import List

from scaler.protocol.python.message import InformationSnapshot, WorkerAdapterCommand, WorkerAdapterHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerGroupCapabilities, WorkerGroupState


class ScalingController:
    """
    Stateless scaling controller interface.

    All state (worker groups, capabilities) is owned by WorkerAdapterController and passed in as parameters.
    Controllers return commands rather than mutating internal state.
    """

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        information_snapshot: InformationSnapshot,
        adapter_heartbeat: WorkerAdapterHeartbeat,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
    ) -> List[WorkerAdapterCommand]:
        """
        Pure function: state in, commands out.

        Returns a list of WorkerAdapterCommands. Commands are either all start or all shutdown, never mixed.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        """Pure function: state in, status out."""
        raise NotImplementedError()
