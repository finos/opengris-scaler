from scaler.protocol.python.message import InformationSnapshot, WorkerAdapterCommandResponse, WorkerAdapterHeartbeat
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import CommandSender, ScalingController


class NoScalingController(ScalingController):
    def __init__(self):
        pass

    def register_command_sender(self, sender: CommandSender) -> None:
        pass

    async def on_snapshot(
        self, information_snapshot: InformationSnapshot, adapter_heartbeat: WorkerAdapterHeartbeat
    ) -> None:
        pass

    def on_command_response(self, response: WorkerAdapterCommandResponse) -> None:
        pass

    def get_status(self) -> ScalingManagerStatus:
        return ScalingManagerStatus.new_msg(worker_groups={})
