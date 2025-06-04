import argparse

from scaler.object_storage import object_storage_server


def get_args():
    parser = argparse.ArgumentParser(
        "scaler object storage server", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "--name",
        "-n",
        type=str,
        default="127.0.0.1",
        help="Specify name the server will be listening to. Can be IP or NS record. Default to 127.0.0.1",
    )
    parser.add_argument(
        "--port", "-p", type=str, default="55555", help="Specify port the server will be listening to. Default to 55555"
    )
    return parser.parse_args()


def main():
    args = get_args()
    object_storage_server.run_object_storage_server(args.name, args.port)
