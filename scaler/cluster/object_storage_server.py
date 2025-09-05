import logging
import multiprocessing
from typing import Tuple

from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.object_storage_config import ObjectStorageConfig


class ObjectStorageServerProcess(multiprocessing.get_context("fork").Process):  # type: ignore[misc]
    def __init__(
        self,
        storage_address: ObjectStorageConfig,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_format: str,
    ):
        multiprocessing.Process.__init__(self, name="ObjectStorageServer")

        self._storage_address = storage_address

        self._server = ObjectStorageServer(logging_level, logging_format, logging_paths)

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to server requests."""
        self._server.wait_until_ready()

    def run(self) -> None:
        logging.info(f"ObjectStorageServer: start and listen to {self._storage_address.to_string()}")

        self._server.run(self._storage_address.host, self._storage_address.port)
