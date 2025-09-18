import argparse
import logging

from scaler.config import ScalerConfig
from scaler.object_storage.object_storage_server import ObjectStorageServer
from scaler.utility.logging.utility import get_logger_info, setup_logger


def get_args():
    parser = argparse.ArgumentParser(
        "scaler object storage server", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML configuration file.")
    parser.add_argument(
        "address", type=str, help="specify the object storage server address to listen to, e.g. tcp://localhost:2345."
    )
    return parser.parse_args()


def main():
    args = get_args()

    scaler_config = ScalerConfig.get_config(args.config, args)

    setup_logger("object_storage_server")

    log_format_str, log_level_str, log_paths = get_logger_info(logging.getLogger())

    ObjectStorageServer().run(
        scaler_config.object_storage.host, scaler_config.object_storage.port, log_level_str, log_format_str, log_paths
    )
