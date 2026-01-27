import logging
import multiprocessing
import os
import select
from typing import Optional, Tuple

from scaler.config.types.object_storage_server import ObjectStorageAddressConfig
from scaler.utility.logging.utility import get_logger_info, setup_logger


class ObjectStorageServerProcess(multiprocessing.get_context("fork").Process):  # type: ignore[misc]
    def __init__(
        self,
        object_storage_address: ObjectStorageAddressConfig,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
        backend: str = "memory",
        redis_config: Optional[dict] = None,
    ):
        multiprocessing.Process.__init__(self, name="ObjectStorageServer")

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        self._object_storage_address = object_storage_address
        self._backend = backend
        self._redis_config = redis_config

        # For Python server: set up ready signaling pipe BEFORE fork
        self._ready_read_fd: Optional[int] = None
        self._ready_write_fd: Optional[int] = None
        self._is_python_server = backend == "redis"

        if self._is_python_server:
            # Create pipe for ready signaling (must be before fork)
            self._ready_read_fd, self._ready_write_fd = os.pipe()

        # Server instance created lazily in run() for Python server
        self._server = None

    def wait_until_ready(self) -> None:
        """Blocks until the object storage server is available to serve requests."""
        if self._is_python_server:
            # Wait for ready signal via pipe
            if self._ready_read_fd is not None:
                try:
                    readable, _, _ = select.select([self._ready_read_fd], [], [], 30.0)
                    if readable:
                        os.read(self._ready_read_fd, 1)
                except (OSError, ValueError):
                    pass
                finally:
                    try:
                        os.close(self._ready_read_fd)
                    except OSError:
                        pass
                    self._ready_read_fd = None
        else:
            # C++ server: need to create instance to wait
            if self._server is None:
                from scaler.object_storage.object_storage_server import ObjectStorageServer

                self._server = ObjectStorageServer()
            self._server.wait_until_ready()

    def run(self) -> None:
        setup_logger(self._logging_paths, self._logging_config_file, self._logging_level)

        backend_info = f" (backend={self._backend})" if self._backend != "memory" else ""
        logging.info(
            f"ObjectStorageServer: start and listen to {self._object_storage_address.to_string()}{backend_info}"
        )

        log_format_str, log_level_str, logging_paths = get_logger_info(logging.getLogger())

        if self._is_python_server:
            # Close read end in child - we only write from here
            if self._ready_read_fd is not None:
                try:
                    os.close(self._ready_read_fd)
                except OSError:
                    pass
                self._ready_read_fd = None

            from scaler.object_storage.python_object_storage_server import PythonObjectStorageServer, create_backend

            storage_backend = create_backend("redis", self._redis_config)
            server = PythonObjectStorageServer(storage_backend)

            # Pass the write FD to the server for signaling ready
            server._ready_write_fd = self._ready_write_fd

            server.run(
                self._object_storage_address.host,
                self._object_storage_address.port,
                self._object_storage_address.identity,
                log_level_str,
                log_format_str,
                logging_paths,
                multiprocessing_ready=False,  # We handle FDs ourselves
            )
        else:
            from scaler.object_storage.object_storage_server import ObjectStorageServer

            self._server = ObjectStorageServer()
            self._server.run(
                self._object_storage_address.host,
                self._object_storage_address.port,
                self._object_storage_address.identity,
                log_level_str,
                log_format_str,
                logging_paths,
            )
