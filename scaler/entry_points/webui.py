import argparse

from scaler.config.loader import load_config
from scaler.config.section.webui import WebUIConfig
from scaler.ui.webui import start_webui


def get_args():
    parser = argparse.ArgumentParser(
        "web ui for scaler monitoring", formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--config", "-c", type=str, default=None, help="Path to the TOML configuration file.")
    parser.add_argument("--web-host", type=str, help="host for webserver to connect to")
    parser.add_argument("--web-port", type=int, help="port for webserver to connect to")
    parser.add_argument("monitor_address", nargs="?", type=str, help="scheduler monitor address to connect to")
    return parser.parse_args()


def main():
    args = get_args()

    webui_config = load_config(WebUIConfig, args.config, args, section_name="webui")

    assert webui_config.monitor_address is not None, "scheduler monitor address has to be set"

    start_webui(webui_config.monitor_address.to_address(), webui_config.web_host, webui_config.web_port)
