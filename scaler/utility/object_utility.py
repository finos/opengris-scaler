import hashlib
import pickle
from typing import Tuple

SERIALIZER_INDICATOR = b"serializer"
POSTFIX_SERIALIZER = hashlib.md5(SERIALIZER_INDICATOR).digest()


def generate_object_id(identity: bytes, object_bytes: bytes) -> bytes:
    identity_hash = hashlib.md5(identity).digest()
    object_hash = hashlib.md5(object_bytes).digest()
    return identity_hash[:8] + object_hash


def split_object_id(object_id: bytes) -> Tuple[bytes, bytes]:
    return object_id[:7], object_id[8:]


def generate_serializer_object_id(source: bytes) -> bytes:
    return generate_object_id(source, SERIALIZER_INDICATOR)


def is_object_id_serializer(object_id: bytes) -> bool:
    return object_id.endswith(POSTFIX_SERIALIZER)


def serialize_failure(exp: Exception) -> bytes:
    return pickle.dumps(exp, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize_failure(result: bytes) -> Exception:
    return pickle.loads(result)
