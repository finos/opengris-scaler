import enum


class AllocatePolicyStrategy(enum.Enum):
    CAPABILITY = "capability"
    EVEN_LOAD = "even_load"

    def __str__(self):
        return self.name
