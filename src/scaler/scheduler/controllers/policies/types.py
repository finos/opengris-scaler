import enum


class PolicyEngineType(enum.Enum):
    SIMPLE = "simple"
    ADVANCE = "advance"

    def __str__(self):
        return self.name
