import abc
import hashlib
import uuid
from typing import Optional


class Identifier(bytes, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __repr__(self) -> str:
        raise NotImplementedError()


class ClientID(Identifier):
    def __repr__(self) -> str:
        return f"ClientID({self.decode()})"

    @staticmethod
    def generate_client_id(name: Optional[str] = None) -> "ClientID":
        unique_client_tag = uuid.uuid4().bytes.hex()

        if name is None:
            return ClientID(f"Client|{unique_client_tag}".encode())
        else:
            return ClientID(f"Client|{name}|{unique_client_tag}".encode())


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
        unique_worker_tag = uuid.uuid4().bytes.hex()
        return WorkerID(f"Worker|{name}|{unique_worker_tag}".encode())


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

    Object IDs are built from 2x16-bytes parts:

    - the first 16-bytes identify the owner of the object (the Scaler client's hash);
    - the second 16-bytes identify the object instance: a hash of its serialized content for
      content-addressed client objects (see ``generate_object_id_from_payload``), or a random
      unique tag otherwise (see ``generate_object_id``), or a fixed tag for the serializer.
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
    def generate_object_id_from_payload(owner: ClientID, payload: bytes) -> "ObjectID":
        """Content-addressed object ID: the same payload from the same owner always maps to the same
        ID, so the client can skip re-uploading (and re-instructing) an object whose serialized
        content has not changed -- the object storage server is keyed by object ID.

        The content tag is an md5 digest of the payload. md5 is not collision-resistant against a
        deliberately crafted adversary, but (a) accidental collisions between distinct real payloads
        are negligible (~2**-64 by the birthday bound over 128 bits), and (b) the owner-hash prefix
        namespaces IDs per client, so even a crafted collision could only affect the crafting
        client's own objects.
        """
        owner_hash = hashlib.md5(owner).digest()
        content_tag = hashlib.md5(payload).digest()
        return ObjectID(owner_hash + content_tag)

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
