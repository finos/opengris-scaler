import multiprocessing
import os

from scaler.object_storage.object_storage_server import run_object_storage_server
from scaler.utility.object_storage_config import ObjectStorageConfig


class ObjectStorageServerProcess(multiprocessing.get_context("fork").Process):  # type: ignore[misc]
    def __init__(self, storage_address: ObjectStorageConfig):
        multiprocessing.Process.__init__(self, name="ObjectStorageServer")

        self._storage_address = storage_address
        self._on_server_ready_fd = os.eventfd(0, os.EFD_SEMAPHORE)

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to server requests."""
        os.eventfd_read(self._on_server_ready_fd)

    def run(self) -> None:
        run_object_storage_server(self._storage_address.host, self._storage_address.port, self._on_server_ready_fd)
