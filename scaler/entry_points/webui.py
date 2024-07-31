import argparse

from scaler.ui.webui import start_webui


def get_args():
    parser = argparse.ArgumentParser(
        "web ui for scaler monitoring", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("address", type=str, help="scheduler ipc address to connect to")
    parser.add_argument("--host", type=str, default="0.0.0.0", help="host for webserver to connect to")
    parser.add_argument("--port", type=int, default=50001, help="port for webserver to connect to")
    return parser.parse_args()


def main():
    args = get_args()
    start_webui(args.address, args.host, args.port)
