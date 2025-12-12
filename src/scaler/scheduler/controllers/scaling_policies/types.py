import enum

WorkerGroupID = bytes


class ScalingControllerStrategy(enum.Enum):
    NULL = "null"
    VANILLA = "vanilla"
    FIXED_ELASTIC = "fixed_elastic"

    def __str__(self):
        return self.name
