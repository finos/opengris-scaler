from scaler.protocol.python.message import InformationSnapshot
from scaler.protocol.python.status import ScalingManagerStatus
from scaler.scheduler.controllers.policies.simple_policy.scaling.mixins import ScalingController


class NoScalingController(ScalingController):
    def __init__(self):
        pass

    def get_status(self):
        return ScalingManagerStatus.new_msg(worker_groups={})

    async def on_snapshot(self, information_snapshot: InformationSnapshot):
        pass
