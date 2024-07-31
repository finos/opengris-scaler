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
