import abc


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self) -> None:
        raise NotImplementedError()


class Reporter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_status(self) -> object:
        raise NotImplementedError()
