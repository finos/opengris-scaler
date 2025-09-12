import abc
from typing_extensions import Self


class ConfigType(metaclass=abc.ABCMeta):
    """A base class for composite config values that can be parsed and serialized from/to a string."""

    @classmethod
    @abc.abstractmethod
    def from_string(cls, value: str) -> Self:
        pass

    @abc.abstractmethod
    def __str__(self) -> str:
        pass
