import argparse
import logging

from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import setup_logger, get_logger_format_and_level
from scaler.utility.object_storage_config import ObjectStorageConfig


def get_args():
    parser = argparse.ArgumentParser(
        "scaler object storage server", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "address",
        type=ObjectStorageConfig.from_string,
        help="specify the object storage server address to listen to, e.g. tcp://localhost:2345.",
    )
    return parser.parse_args()


def main():
    args = get_args()
    setup_logger("object_storage_server")

    root_logger = logging.getLogger()
    log_format_str, log_level_str = get_logger_format_and_level(root_logger)

    ObjectStorageServer().run(args.address.host, args.address.port, log_level_str, log_format_str)
