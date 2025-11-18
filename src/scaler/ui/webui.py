import logging
from typing import Optional, Tuple
from scaler.ui.util import NICEGUI_MAJOR_VERSION
from scaler.utility.logging.utility import setup_logger


def start_webui(
    address: str,
    host: str,
    port: int,
    logging_paths: Tuple[str, ...],
    logging_config_file: Optional[str],
    logging_level: str,
):

    setup_logger(logging_paths, logging_config_file, logging_level)

    if NICEGUI_MAJOR_VERSION < 3:
        logging.info(f"Detected {NICEGUI_MAJOR_VERSION}. Using GUI v1.")
        from scaler.ui.v1 import start_webui_v1

        start_webui_v1(address, host, port)
    else:
        logging.info(f"Detected {NICEGUI_MAJOR_VERSION}. Using GUI v2.")
        from scaler.ui.v2 import start_webui_v2

        start_webui_v2(address, host, port)
