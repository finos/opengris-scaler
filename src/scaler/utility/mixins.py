import abc


class Looper(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def routine(self):
        raise NotImplementedError()


class Reporter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_status(self):
        raise NotImplementedError()
