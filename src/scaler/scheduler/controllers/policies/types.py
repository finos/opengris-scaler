import enum


class PolicyEngineType(enum.Enum):
    SIMPLE = "simple"

    def __str__(self):
        return self.name
