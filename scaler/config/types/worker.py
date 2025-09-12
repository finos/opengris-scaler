import dataclasses
from typing import List, Dict
from typing_extensions import Self

from scaler.config.mixins import ConfigType


@dataclasses.dataclass
class WorkerNames(ConfigType):
    """Parses a comma-separated string of worker names into a list."""

    names: List[str]

    @classmethod
    def from_string(cls, value: str) -> Self:
        if not value:
            return cls([])
        names = [name.strip() for name in value.split(",")]
        return cls(names)

    def __str__(self) -> str:
        return ",".join(self.names)

    def __len__(self) -> int:
        return len(self.names)


@dataclasses.dataclass
class WorkerCapabilities(ConfigType):
    """Parses a string of worker capabilities."""

    capabilities: Dict[str, int]

    @classmethod
    def from_string(cls, value: str) -> Self:
        capabilities: Dict[str, int] = {}
        if not value:
            return cls(capabilities)
        for item in value.split(","):
            name, _, value = item.partition("=")
            if value != "":
                try:
                    capabilities[name.strip()] = int(value)
                except ValueError:
                    raise ValueError(f"Invalid capability value for '{name}'. Expected an integer, but got '{value}'.")
            else:
                capabilities[name.strip()] = -1
        return cls(capabilities)

    def __str__(self) -> str:
        items = []
        for name, cap in self.capabilities.items():
            if cap == -1:
                items.append(name)
            else:
                items.append(f"{name}={cap}")
        return ",".join(items)
