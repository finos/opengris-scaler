import abc
from typing import Callable, Coroutine

from scaler.protocol.python.message import (
    InformationSnapshot,
    WorkerAdapterCommand,
    WorkerAdapterCommandResponse,
    WorkerAdapterHeartbeat,
)
from scaler.protocol.python.status import ScalingManagerStatus

# Type alias for the command sender callback
CommandSender = Callable[[WorkerAdapterCommand], Coroutine[None, None, None]]


class ScalingController:
    """
    Stateful scaling controller interface.

    Each controller owns its own state (worker groups, capabilities).
    Commands are sent via a registered callback to the WorkerAdapterController.
    """

    @abc.abstractmethod
    def register_command_sender(self, sender: CommandSender) -> None:
        """
        Register a callback function to send commands to the worker adapter.

        Args:
            sender: An async function that sends a WorkerAdapterCommand.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    async def on_snapshot(
        self, information_snapshot: InformationSnapshot, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> None:
        """
        Process an information_snapshot and send commands via the registered sender.

        This method evaluates the current state against the information_snapshot and
        sends any necessary scaling commands.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def on_command_response(self, response: WorkerAdapterCommandResponse) -> None:
        """
        Handle a response to a previously sent command.

        Updates internal state based on the response (e.g., adding or removing
        worker groups from tracking).
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_status(self) -> ScalingManagerStatus:
        """Return the current scaling status."""
        raise NotImplementedError()
