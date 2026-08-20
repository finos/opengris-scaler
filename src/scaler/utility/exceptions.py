class TaskNotFoundError(Exception):
    pass


class WorkerDiedError(Exception):
    pass


class NoWorkerError(Exception):
    pass


class DisconnectedError(Exception):
    pass


class ProcessorDiedError(Exception):
    pass


class SchedulerError(Exception):
    """The scheduler failed while it was applying a transition for this task, so the task cannot continue.

    This is a scheduler-side fault rather than a failure of the task's own code. It is reported as an ordinary failed
    result so that every existing client understands it: a new TaskResultType would reach the client's final `else`
    and raise, after the future was already popped, taking down the whole client agent."""

    pass


class TaskExceptionNotSerializableError(Exception):
    """A task raised an exception that could not be pickled back to the client; the message preserves the
    original exception's type name and string so the failure is still meaningful."""

    pass


class DeserializeObjectError(Exception):
    pass


class MissingObjects(Exception):
    pass


class ClientCancelledException(Exception):
    pass


class ClientShutdownException(Exception):
    pass


class ClientQuitException(Exception):
    pass


class ObjectStorageException(Exception):
    pass


class CapnpDeserializationError(Exception):
    """Raised when scaler.protocol.capnp fails to decode a wire message,
    including unknown enum ordinals from a newer schema."""

    pass
