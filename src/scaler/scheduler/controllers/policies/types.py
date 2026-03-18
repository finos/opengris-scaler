import enum


class PolicyEngineType(enum.Enum):
    SIMPLE = "simple"
    WATERFALL_V1 = "waterfall_v1"

    def __str__(self):
        return self.name
