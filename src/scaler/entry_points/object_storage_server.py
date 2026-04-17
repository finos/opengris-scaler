# PYTHON_ARGCOMPLETE_OK
import logging
import sys

from scaler.config.section.object_storage_server import ObjectStorageServerConfig
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import get_logger_info, setup_logger


def main():
    oss_config = ObjectStorageServerConfig.parse("Scaler Object Storage Server", "object_storage_server")

    setup_logger(
        oss_config.logging_config.paths, oss_config.logging_config.config_file, oss_config.logging_config.level
    )

    log_format_str, log_level_str, log_paths = get_logger_info(logging.getLogger())

    try:
        ObjectStorageServer().run(
            oss_config.bind_address.host,
            oss_config.bind_address.port,
            oss_config.identity,
            log_level_str,
            log_format_str,
            log_paths,
        )
    except KeyboardInterrupt:
        sys.exit(0)
