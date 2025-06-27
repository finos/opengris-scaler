import argparse

from scaler.object_storage.object_storage_server import run_object_storage_server
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
    run_object_storage_server(args.address.host, args.address.port)
