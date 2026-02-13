import abc
from typing import List, Tuple

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.types import WorkerGroupCapabilities, WorkerGroupState


class ScalingController:
    """
    Stateless scaling controller interface.

    All state (worker groups, capabilities) is owned by WorkerAdapterController and passed in as parameters.
    Controllers return commands and updated state rather than mutating internal state.
    """

    @abc.abstractmethod
    def get_scaling_commands(
        self,
        snapshot: InformationSnapshot,
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
    def on_scaling_command_response(
        self,
        response: WorkerAdapterCommandResponse,
        worker_groups: WorkerGroupState,
        worker_group_capabilities: WorkerGroupCapabilities,
    ) -> Tuple[WorkerGroupState, WorkerGroupCapabilities]:
        """
        Pure function: takes current state + response, returns updated state.

        Returns (updated_worker_groups, updated_worker_group_capabilities).
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self, worker_groups: WorkerGroupState) -> ScalingManagerStatus:
        """Pure function: state in, status out."""
        raise NotImplementedError()
