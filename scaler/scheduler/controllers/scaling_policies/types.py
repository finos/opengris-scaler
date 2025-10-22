import enum

WorkerGroupID = bytes


class ScalingControllerStrategy(enum.Enum):
    NULL = "null"
    VANILLA = "vanilla"
    FIXED_ELASTIC = "fixed_elastic"
