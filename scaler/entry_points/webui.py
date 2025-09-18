import argparse

from scaler.config import ScalerConfig
from scaler.ui.webui import start_webui


def get_args():
    parser = argparse.ArgumentParser(
        "web ui for scaler monitoring", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", type=str, default=None, help="Path to the TOML configuration file.")
    parser.add_argument("sched_address", type=str, help="scheduler ipc address to connect to")
    parser.add_argument("--web_host", type=str, default="0.0.0.0", help="host for webserver to connect to")
    parser.add_argument("--web_port", type=int, default=50001, help="port for webserver to connect to")
    return parser.parse_args()


def main():
    args = get_args()

    scaler_config = ScalerConfig.get_config(args.config, args)

    start_webui(scaler_config.webui.sched_address, scaler_config.webui.web_host, scaler_config.webui.web_port)
