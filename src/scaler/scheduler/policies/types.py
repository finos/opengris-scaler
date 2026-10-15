import enum


class PolicyEngineType(enum.Enum):
    SIMPLE = "simple"
    WATERFALL_V1 = "waterfall_v1"

    def __str__(self):
        return self.name


class AllocatePolicyStrategy(enum.Enum):
    CAPABILITY = "capability"
    EVEN_LOAD = "even_load"

    def __str__(self):
        return self.name


class ScalingPolicyStrategy(enum.Enum):
    NO = "no"
    VANILLA = "vanilla"
    CAPABILITY = "capability"

    def __str__(self):
        return self.name
