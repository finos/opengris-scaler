import abc
from typing import Any


class Serializer(metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def serialize(obj: Any) -> bytes:
        """
        Serialize the object to bytes, this serialization method is used to call for function object and EACH argument
        object and function result object, for example:

        def add(a, b):
            return a + b

        client.submit(add, 1, 2)

        The add function and the arguments 1 and 2 will be serialized and sent to the worker, and the result of the a+b
        will be serialized and sent back to the client, client will use deserialize function below to deserialize

        :param obj: the object to be serialized, can be function object, argument object, or function result object
        :return: serialized bytes of the object
        """

        raise NotImplementedError()

    @staticmethod
    @abc.abstractmethod
    def deserialize(payload: bytes) -> Any:
        """
        Deserialize the bytes to the original object, this de-serialize method is used to deserialize the function
        object bytes and EACH serialized argument and serialized function result.

        :param payload: the serialized bytes of the object, can be function object, argument object, or function result
            object
        :return: any deserialized object
        """
        raise NotImplementedError()
