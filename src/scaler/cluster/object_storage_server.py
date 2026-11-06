import logging
import multiprocessing
from typing import Optional, Tuple

from scaler.cluster.readiness import wait_for_ready_report
from scaler.config.common.security import SecurityConfig
from scaler.config.types.address import AddressConfig
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import get_logger_info
from scaler.utility.process_bootstrap import bootstrap_process

logger = logging.getLogger(__name__)


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
        return wait_for_ready_report(self._ready_receiver, self, "ObjectStorageServer", self._bind_address)

    def run(self) -> None:
        bootstrap_process(
            self._logging_paths, self._logging_config_file, self._logging_level, process_name="object_storage_server"
        )
        logger.info(f"ObjectStorageServer: start and listen to {self._bind_address!r}")

        log_format_str, log_level_str, logging_paths = get_logger_info(logging.getLogger("scaler"))

        tls_cert = self._security_config.tls_cert if self._security_config is not None else None
        tls_key = self._security_config.tls_key if self._security_config is not None else None

        self._server = ObjectStorageServer()
        try:
            self._server.run(
                repr(self._bind_address),
                self._ident,
                log_level_str,
                log_format_str,
                logging_paths,
                tls_cert,
                tls_key,
                lambda bound_address: self._ready_sender.send(AddressConfig.from_string(bound_address)),
            )
        except KeyboardInterrupt:
            logger.info("ObjectStorageServer: received KeyboardInterrupt, shutting down")
