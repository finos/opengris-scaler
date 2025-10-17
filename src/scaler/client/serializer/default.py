import pickle
from typing import Any

import cloudpickle

from scaler.client.serializer.mixins import Serializer


class DefaultSerializer(Serializer):
    @staticmethod
    def serialize(obj: Any) -> bytes:
        return cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def deserialize(payload: bytes) -> Any:
        return cloudpickle.loads(payload)
