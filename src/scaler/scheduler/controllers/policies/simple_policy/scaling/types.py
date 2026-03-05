import enum


class ScalingPolicyStrategy(enum.Enum):
    NO = "no"
    VANILLA = "vanilla"
    FIXED_ELASTIC = "fixed_elastic"
    CAPABILITY = "capability"

    def __str__(self):
        return self.name
