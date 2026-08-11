import pickle

from scaler.utility.exceptions import TaskExceptionNotSerializableError


def serialize_failure(exp: Exception) -> bytes:
    try:
        return pickle.dumps(exp, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        # The task's exception is not picklable (it holds a lock/socket/file handle, is a locally-defined
        # class, etc.). Degrade to an error that keeps the original type name and message so the client
        # still gets a meaningful failure, instead of the serialize raising here, killing the processor,
        # and surfacing an opaque ProcessorDiedError.
        try:
            detail = f"{type(exp).__name__}: {exp}"
        except Exception:
            detail = type(exp).__name__
        return pickle.dumps(TaskExceptionNotSerializableError(detail), protocol=pickle.HIGHEST_PROTOCOL)


def deserialize_failure(result: bytes) -> Exception:
    return pickle.loads(result)
