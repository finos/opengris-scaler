import logging
import multiprocessing
import socket
import time
from typing import Optional, Tuple

from scaler.config.types.address import AddressConfig
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import get_logger_info, setup_logger


class ObjectStorageServerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        bind_address: AddressConfig,
        identity: str,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
    ):
        super().__init__(name="ObjectStorageServer")

        self._ident = identity

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        self._bind_address = bind_address

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to server requests."""
        host = self._bind_address.host
        port = self._bind_address.port

        start_time = time.time()
        while time.time() - start_time < 30:
            try:
                # Try to connect to the port
                with socket.create_connection((host, port), timeout=1):
                    return
            except (ConnectionRefusedError, socket.timeout, OSError):
                time.sleep(0.1)

        raise TimeoutError(f"ObjectStorageServer at {host}:{port} failed to start within 30 seconds")

    def run(self) -> None:
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)
        logging.info(f"ObjectStorageServer: start and listen to {self._bind_address!r}")

        log_format_str, log_level_str, logging_paths = get_logger_info(logging.getLogger())

        self._server = ObjectStorageServer()
        try:
            self._server.run(
                self._bind_address.host,
                self._bind_address.port,
                self._ident,
                log_level_str,
                log_format_str,
                logging_paths,
            )
        except KeyboardInterrupt:
            logging.info("ObjectStorageServer: received KeyboardInterrupt, shutting down")
