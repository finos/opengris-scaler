import logging

import uvicorn  # pyright: ignore[reportMissingImports]

from scaler.config.section.webgui import WebGUIConfig
from scaler.ui.app import create_app
from scaler.utility.logging.utility import setup_logger


def start_webgui(config: WebGUIConfig) -> None:
    setup_logger(config.logging_config.paths, config.logging_config.config_file, config.logging_config.level)

    app = create_app(config)
    logging.info("Web GUI is now listening on: http://%s:%s", config.web_host, config.web_port)
    uvicorn.run(app, host=config.web_host, port=config.web_port)
