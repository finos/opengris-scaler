import pickle


def serialize_failure(exp: Exception) -> bytes:
    return pickle.dumps(exp, protocol=pickle.HIGHEST_PROTOCOL)


def deserialize_failure(result: bytes) -> Exception:
    return pickle.loads(result)
