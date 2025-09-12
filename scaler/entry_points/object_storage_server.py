import argparse
import logging

from scaler.config.config import ObjectStorageServerConfig
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import get_logger_info, setup_logger


def get_args():
    parser = argparse.ArgumentParser(
        "scaler_object_storage_server", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML configuration file.")

    parser.add_argument(
        "object_storage_address",
        nargs="?",
        type=str,
        help="specify the object storage server address to listen to, e.g. tcp://localhost:2345.",
    )
    return parser.parse_args()


def main():
    args = get_args()

    oss_config = ObjectStorageServerConfig.get_config(args.config, args)

    setup_logger()

    log_format_str, log_level_str, log_paths = get_logger_info(logging.getLogger())

    ObjectStorageServer().run(
        oss_config.object_storage_address.host,
        oss_config.object_storage_address.port,
        oss_config.object_storage_address.identity,
        log_level_str,
        log_format_str,
        log_paths,
    )
