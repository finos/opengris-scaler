import abc

from scaler.protocol.python.message import InformationSnapshot
from scaler.utility.mixins import Reporter


class ScalingController(Reporter):
    @abc.abstractmethod
    async def on_snapshot(self, snapshot: InformationSnapshot):
        raise NotImplementedError()
