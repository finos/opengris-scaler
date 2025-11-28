import logging

from scaler.config.section.webui import WebUIConfig
from scaler.ui.util import NICEGUI_MAJOR_VERSION
from scaler.utility.logging.utility import setup_logger


def start_webui(config: WebUIConfig):

    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    if NICEGUI_MAJOR_VERSION < 3:
        logging.info(f"Detected {NICEGUI_MAJOR_VERSION}. Using GUI v1.")
        from scaler.ui.v1 import start_webui_v1

        start_webui_v1(config)
    else:
        logging.info(f"Detected {NICEGUI_MAJOR_VERSION}. Using GUI v2.")
        from scaler.ui.v2 import start_webui_v2

        start_webui_v2(config)
