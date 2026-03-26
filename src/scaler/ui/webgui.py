import logging

import uvicorn  # pyright: ignore[reportMissingImports]

from scaler.config.section.webgui import WebGUIConfig
from scaler.ui.app import create_app
from scaler.utility.logging.utility import setup_logger


def start_webgui(config: WebGUIConfig) -> None:
    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    app = create_app(config)
    logging.info(f"Web GUI is now listening on: http://{config.gui_address}")
    uvicorn.run(app, host=config.gui_address.host, port=config.gui_address.port)
