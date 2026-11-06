import logging
import multiprocessing
import multiprocessing.connection
import threading
from typing import Optional, Tuple

from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import get_logger_info
from scaler.utility.process_bootstrap import bootstrap_process

logger = logging.getLogger(__name__)


_READINESS_TIMEOUT_SECONDS = 30


class ObjectStorageServerProcess(multiprocessing.get_context("spawn").Process):  # type: ignore[misc]
    def __init__(
        self,
        bind_address: AddressConfig,
        identity: str,
        logging_paths: Tuple[str, ...],
        logging_level: str,
        logging_config_file: Optional[str],
        security_config: Optional[SecurityConfig] = None,
    ):
        super().__init__(name="ObjectStorageServer")

        self._ident = identity

        self._logging_paths = logging_paths
        self._logging_level = logging_level
        self._logging_config_file = logging_config_file

        self._bind_address = bind_address
        self._security_config = security_config

        self._ready_receiver, self._ready_sender = multiprocessing.get_context("spawn").Pipe(duplex=False)

    def wait_until_ready(self) -> AddressConfig:
        """Blocks until the object storage server listens, returning its bound address.

        The bound address differs from the configured one when that names port 0.
        """
        # the sentinel wakes the wait if the server exits first, without polling its exit code
        ready = multiprocessing.connection.wait([self._ready_receiver, self.sentinel], _READINESS_TIMEOUT_SECONDS)
        if self._ready_receiver in ready:
            return self._ready_receiver.recv()

        if self.sentinel in ready:
            self.join()
            raise RuntimeError(
                f"ObjectStorageServer exited with code {self.exitcode} before listening to {self._bind_address!r}"
            )

        raise TimeoutError(
            f"ObjectStorageServer at {self._bind_address!r} failed to start within {_READINESS_TIMEOUT_SECONDS} seconds"
        )

    def run(self) -> None:
        bootstrap_process(
            self._logging_paths, self._logging_config_file, self._logging_level, process_name="object_storage_server"
        )
        logger.info(f"ObjectStorageServer: start and listen to {self._bind_address!r}")

        log_format_str, log_level_str, logging_paths = get_logger_info(logging.getLogger("scaler"))

        tls_cert = self._security_config.tls_cert if self._security_config is not None else None
        tls_key = self._security_config.tls_key if self._security_config is not None else None

        self._server = ObjectStorageServer()
        threading.Thread(target=self._report_bound_address, daemon=True).start()
        try:
            self._server.run(
                repr(self._bind_address), self._ident, log_level_str, log_format_str, logging_paths, tls_cert, tls_key
            )
        except KeyboardInterrupt:
            logger.info("ObjectStorageServer: received KeyboardInterrupt, shutting down")

    def _report_bound_address(self) -> None:
        bound_address = self._server.wait_until_ready()
        if bound_address is None:  # the server stopped before binding and logged why
            return

        self._ready_sender.send(AddressConfig.from_string(bound_address))
