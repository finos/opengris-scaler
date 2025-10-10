import abc
import hashlib
import os
import uuid
from typing import Optional


class Identifier(bytes, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError()

    def extend(self, extra: str):
        return self.__class__(self + extra.encode())  # type: ignore[abstract]


class ClientID(Identifier):
    def __repr__(self) -> str:
        return f"ClientID({self.decode()})"

    @staticmethod
    def generate_client_id(name: Optional[str] = None) -> "ClientID":
        if name is None:
            name = uuid.uuid4().bytes.hex()

        return ClientID(f"{os.getpid()}|Client|{name}".encode())


class WorkerID(Identifier):
    def __repr__(self) -> str:
        return f"WorkerID({self.decode()})"

    def is_valid(self) -> bool:
        return self != _INVALID_WORKER_ID

    @staticmethod
    def invalid_worker_id() -> "WorkerID":
        return _INVALID_WORKER_ID

    @staticmethod
    def generate_worker_id(name: str) -> "WorkerID":
        return WorkerID(f"{os.getpid()}|Worker|{name}|{uuid.uuid4().bytes.hex()}".encode())


_INVALID_WORKER_ID = WorkerID(b"")


class ProcessorID(Identifier):
    def __repr__(self) -> str:
        return f"ProcessorID({self.hex()})"

    @staticmethod
    def generate_processor_id() -> "ProcessorID":
        return ProcessorID(uuid.uuid4().bytes)


class TaskID(Identifier):
    def __repr__(self) -> str:
        return f"TaskID({self.hex()})"

    @staticmethod
    def generate_task_id() -> "TaskID":
        return TaskID(uuid.uuid4().bytes)


class ObjectID(bytes):
    SERIALIZER_TAG = hashlib.md5(b"serializer").digest()

    """
    Scaler 32-bytes object IDs.

    Object ID are built from 2x16-bytes parts:

    - the first 16-bytes uniquely identify the owner of the object (i.e. the Scaler client's hash);
    - the second 16-bytes uniquely identify the object's content.
    """

    def __new__(cls, value: bytes):
        if len(value) != 32:
            raise ValueError("Scaler object ID must be 32 bytes.")

        return super().__new__(cls, value)

    @staticmethod
    def generate_object_id(owner: ClientID) -> "ObjectID":
        owner_hash = hashlib.md5(owner).digest()
        unique_object_tag = uuid.uuid4().bytes
        return ObjectID(owner_hash + unique_object_tag)

    @staticmethod
    def generate_serializer_object_id(owner: ClientID) -> "ObjectID":
        owner_hash = hashlib.md5(owner).digest()
        return ObjectID(owner_hash + ObjectID.SERIALIZER_TAG)

    def owner_hash(self) -> bytes:
        return self[:16]

    def object_tag(self) -> bytes:
        return self[16:]

    def is_serializer(self) -> bool:
        return self.object_tag() == ObjectID.SERIALIZER_TAG

    def is_owner(self, owner: ClientID) -> bool:
        return hashlib.md5(owner).digest() == self.owner_hash()

    def __repr__(self) -> str:
        return f"ObjectID(owner_hash={self.owner_hash().hex()}, object_tag={self.object_tag().hex()})"
